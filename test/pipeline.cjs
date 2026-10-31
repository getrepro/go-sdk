// Run from go-sdk. Uses existing API/collector code unchanged and disposable data stores.
const assert = require('node:assert/strict');
const { createRequire } = require('node:module');
const { resolve, join } = require('node:path');
const { mkdtemp, rm } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { randomBytes } = require('node:crypto');
const run = promisify(execFile);
const root = resolve(__dirname, '../..');
const apiRoot = join(root, 'api');
const collectorRoot = join(root, 'log-collector');
const apiRequire = createRequire(join(apiRoot, 'package.json'));
const collectorRequire = createRequire(join(collectorRoot, 'package.json'));
const { GenericContainer, MongoDBContainer } = apiRequire('testcontainers');
const { ClickHouseContainer } = apiRequire('@testcontainers/clickhouse');
const { createClient } = apiRequire('@clickhouse/client');
const { getConnectionToken } = apiRequire('@nestjs/mongoose');
const request = apiRequire('supertest');
const stop = [];

async function start(container) {
  const running = await container.start();
  stop.push(() => running.stop());
  return running;
}

async function main() {
  const temp = await mkdtemp(join(tmpdir(), 'repro-go-pipeline-'));
  stop.push(() => rm(temp, { recursive: true, force: true }));
  const binary = join(temp, 'app');
  await run(join(root, 'go-sdk/bin/repro-go'), ['build', '-o', binary, '.'], {
    cwd: join(root, 'go-sdk/test/fixtures/integrations'), timeout: 300000, maxBuffer: 4 << 20,
  });
  const mongo = await start(new MongoDBContainer('mongo:7.0'));
  const pg = await start(new GenericContainer('postgres:16-alpine').withEnvironment({
    POSTGRES_PASSWORD: 'test-only', POSTGRES_DB: 'repro_test',
  }).withExposedPorts(5432));
  const redis = await start(new GenericContainer('redis:7.4-alpine').withExposedPorts(6379));
  const ch = await start(new ClickHouseContainer('clickhouse/clickhouse-server:25.6-alpine')
    .withDatabase('repro').withUsername('repro').withPassword('test-only'));
  const mongoUrl = `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}/test?directConnection=true`;
  const token = randomBytes(32).toString('hex');
  Object.assign(process.env, {
    NODE_ENV: 'ci', REPRO_ENV_FILE: '/dev/null', MONGO_URI: mongoUrl, MONGODB_URI: mongoUrl, MONGO_TLS: 'false',
    DATA_ENCRYPTION_KEY: randomBytes(32).toString('base64'), DATA_ENCRYPTION_ENABLED: 'false',
    APP_ENCRYPTION_DISABLED: '1', APP_USER_JWT_SECRET: randomBytes(32).toString('hex'), INGEST_CONTROL_TOKEN: token,
    CLICKHOUSE_ENABLED: 'true', CLICKHOUSE_URL: ch.getHttpUrl(), CLICKHOUSE_DATABASE: 'repro',
    CLICKHOUSE_USERNAME: 'repro', CLICKHOUSE_PASSWORD: 'test-only',
  });
  const chClient = createClient({ ...ch.getClientOptions(), clickhouse_settings: {
    async_insert: 1, wait_for_async_insert: 1, async_insert_busy_timeout_ms: 20,
  } });
  stop.push(() => chClient.close());
  const collector = collectorRequire('fastify')({ bodyLimit: 16 << 20 });
  const { runMigrations } = collectorRequire('./dist/storage/clickhouse/migrations/runner');
  await runMigrations({ client: chClient, logger: collector.log, migrationsDir: join(collectorRoot, 'migrations/clickhouse') });
  apiRequire('ts-node').register({ project: join(apiRoot, 'tsconfig.json'), transpileOnly: false });
  const { createE2eApp, closeE2eApp, bootstrapWorkspaceAndLoginAdmin } = apiRequire('./test/support/e2e-app.helpers.ts');
  const context = await createE2eApp();
  stop.push(() => closeE2eApp(context));
  await context.app.listen(0, '127.0.0.1');
  const owner = await bootstrapWorkspaceAndLoginAdmin(context.app);
  const headers = { 'x-app-id': owner.appId, 'x-app-secret': owner.appSecret };
  const auth = { 'x-app-id': owner.appId, Authorization: `Bearer ${owner.adminJwt}` };
  const policy = process.env.REPRO_PIPELINE_POLICY === '1';
  const live = process.env.REPRO_PIPELINE_LIVE === '1';
  assert(!(policy && live), 'run live and policy scenarios independently');
  const prefix = `S_${Date.now()}_go_${policy ? 'policy' : 'raw'}_`;
  const workflows = policy ? ['redis'] : ['sql', 'pgx', 'redis', 'mongo'];
  if (policy) {
    const target = { appId: owner.appId, serviceName: 'go-integrations', scope: '.' };
    await request(context.app.getHttpServer())
      .put(`/v1/apps/${owner.appId}/privacy/environments/prod/policy`).set(auth).query({ serviceName: 'go-integrations' })
      .send({ state: 'approved', proposal: { schemaVersion: 1, generatedAt: new Date().toISOString(),
        generator: { kind: 'e2e', mode: 'init' }, target, strategyGroups: [], rules: [], exceptions: [] },
        policy: { schemaVersion: 4, environment: 'prod', strength: 'medium', groups: [{ statements: [
          { sid: 'body', when: { scope: 'FIELD', selector: 'response.body' }, apply: { action: 'KEEP_EXACT' } },
          { sid: 'return', when: { scope: 'FUNCTION', selector: 'largeResponse', target: 'trace.returnValue' }, apply: { action: 'KEEP_EXACT' } },
          { sid: 'config', when: { scope: 'FIELD', selector: 'apiConfigService' }, apply: { action: 'DROP' } },
          { sid: 'email', when: { scope: 'FIELD', selector: 'email' }, apply: { action: 'TOKENIZE' } },
        ] }] }, policyContext: target, source: { mode: 'init' }, branchPatterns: ['main'], deployOnMerge: true,
      }).expect(200);
  }
  for (const name of live ? [] : workflows) {
    await request(context.app.getHttpServer()).post('/v1/live-sessions').set(headers)
      .send({ sessionId: prefix + name, captureMode: 'backend-only' }).expect(201);
  }
  const { createConfig } = collectorRequire('./dist/config/env');
  const { createPostIngestHandler } = collectorRequire('./dist/http/routes/ingest');
  const { ClickHouseEventsRepository } = collectorRequire('./dist/storage/clickhouse/repositories/events.repository');
  const { createClickHouseEventWriter } = collectorRequire('./dist/storage/clickhouse/writer');
  const config = createConfig({ INGEST_CONTROL_TOKEN: token,
    CAPTURE_ACCOUNTING_API_URL: `${await context.app.getUrl()}/v1/internal/capture-usage` });
  const writer = createClickHouseEventWriter(collector.log, new ClickHouseEventsRepository(chClient), {
    batchSize: 1000, flushMs: 10, maxBatchBytes: 4 << 20, maxQueueBytes: 64 << 20,
    maxQueueItems: 10000, maxRetries: 1, retryBaseMs: 5,
  });
  // Exercise the real durable oversized-event path. Kafka propagation has a
  // separate real-broker suite; this test does not claim broker-path coverage.
  const envelopes = [];
  const ingest = createPostIngestHandler(config, collector, {
    oversizedEventWriter: writer,
    publishKafkaMessage: async () => { throw Object.assign(new Error('Broker: Message size too large'), { code: 10 }); },
  });
  collector.post('/v1/ingest/events', async (req, reply) => {
    const receipt = await ingest(req, reply);
    if (reply.statusCode === 202) envelopes.push(...req.body.events);
    return receipt;
  });
  await collector.listen({ host: '127.0.0.1', port: 0 });
  stop.push(async () => { await writer.flush(); await collector.close(); });
  const appEnv = {
    ...process.env, POSTGRES_URL: `postgres://postgres:test-only@${pg.getHost()}:${pg.getMappedPort(5432)}/repro_test?sslmode=disable`,
    MONGO_URL: mongoUrl, REDIS_ADDR: `${redis.getHost()}:${redis.getMappedPort(6379)}`,
    REPRO_TEST_COLLECTOR: `http://127.0.0.1:${collector.server.address().port}`,
    REPRO_TEST_TENANT: owner.tenantId, REPRO_TEST_APP: owner.appId, REPRO_TEST_SECRET: owner.appSecret,
    REPRO_TEST_SESSION_PREFIX: prefix, REPRO_TEST_LARGE_VALUE: '1',
    REPRO_TEST_WORKFLOWS: workflows.join(','), REPRO_TEST_POLICY: policy ? '1' : '0', REPRO_TEST_API: await context.app.getUrl(),
    REPRO_TEST_LIVE: live ? '1' : '0',
  };
  const { stdout } = await run(binary, [], { timeout: 120000, maxBuffer: 16 << 20, env: appEnv });
  const business = JSON.parse(stdout).responses;
  await writer.flush();
  const db = context.moduleFixture.get(getConnectionToken()).db;
  if (live) {
    assert.equal(JSON.parse(stdout).businessResponsesOK, true);
    const captured = envelopes.filter(event => event.event_type === 'backend_request');
    const byPath = path => captured.filter(event => event.payload.request.path === path);
    for (const path of ['/zero', '/expired', '/wrong-group']) assert.equal(byPath(path).length, 0, `${path} ignored sampling`);
    for (const path of ['/target', '/group', '/continued']) assert.equal(byPath(path).length, 1, `${path} missing capture`);
    assert.equal(byPath('/full').length, 6, 'concurrent live requests lost');
    const sessions = new Set(captured.map(event => event.session_id));
    assert.equal(sessions.size, 3, 'same actor admitted multiple billable segments');
    const fullSession = byPath('/full')[0].session_id;
    assert.equal(byPath('/continued')[0].session_id, fullSession, 'admitted chain was resampled');
    for (const event of captured) assert.equal(event.actor_id,
      event.payload.request.path === '/continued' ? 'full' : event.payload.request.path.slice(1), 'capture actor differs from admitted actor');
    const lateExits = envelopes.flatMap(event => event.event_type === 'trace_batch' ? event.payload.trace : [])
      .filter(trace => trace.fn.endsWith('.largeResponse') && trace.type === 'exit');
    assert.equal(lateExits.length, 6, 'work after HTTP response was lost');
    for (const sessionId of sessions) {
      await request(context.app.getHttpServer()).post(`/v1/sessions/${sessionId}/reconstruct-from-clickhouse`).set(headers).expect(201);
      const filter = { tenantId: owner.tenantId, sessionId };
      const expected = captured.filter(event => event.session_id === sessionId).length;
      assert.equal(await db.collection('requests').countDocuments(filter), expected);
      assert(await db.collection('changes').countDocuments(filter) >= 2 * expected);
      assert(await db.collection('traces').countDocuments(filter) > 0);
      console.log(`PASS live pipeline, actor correlation and late work: ${sessionId} (${expected} requests)`);
    }
    const before = envelopes.length;
    await db.collection('capture_periods').updateOne({ tenantId: owner.tenantId,
      month: new Date().toISOString().slice(0, 7) }, { $set: { completed: 1000 } });
    const denied = await run(binary, [], { timeout: 120000, maxBuffer: 16 << 20,
      env: { ...appEnv, REPRO_TEST_QUOTA_DENIED: '1' } });
    assert.equal(JSON.parse(denied.stdout).businessResponsesOK, true);
    assert.equal(envelopes.length, before, 'quota denial still uploaded evidence');
    console.log('PASS live zero sampling, actor/group targets, expiry, concurrent admission, sticky chains, quota denial without host failure');
  }
  for (const name of live ? [] : Object.keys(business)) {
    const sessionId = prefix + name;
    const reconstructed = await request(context.app.getHttpServer())
      .post(`/v1/sessions/${sessionId}/reconstruct-from-clickhouse`).set(headers).expect(201);
    assert(reconstructed.body.backendIngested > 0);
    const filter = { tenantId: owner.tenantId, sessionId };
    const req = await db.collection('requests').findOne(filter);
    assert(req, `${name}: request lost during reconstruction`);
    assert.equal(req.status, 200);
    assert.equal(req.method, 'GET');
    assert(req.url.endsWith(`/${name}`));
    assert(req.respBodyValueCapture?.valueId, `${name}: full response reference missing`);
    const response = await request(context.app.getHttpServer())
      .get(`/v1/sessions/${sessionId}/trace-values/${req.respBodyValueCapture.valueId}`).set(auth).expect(200);
    if (!policy) assert.deepEqual(response.body.value, business[name], `${name}: full response changed in storage`);
    else {
      const value = response.body.value;
      assert.equal(value.document, business[name].document, 'policy lost exact business document');
      assert.deepEqual(value.result, business[name].result, 'policy changed database business result');
      assert(!JSON.stringify(value).includes('customer@example.test'), 'policy leaked email');
      assert(!JSON.stringify(value).includes('never-leave-the-app'), 'policy leaked infrastructure secret');
      const stored = await db.collection('trace_values').find(filter).limit(500).toArray();
      assert(!JSON.stringify(stored).includes('never-leave-the-app'), 'trace values bypassed approved policy');
    }
    assert(await db.collection('traces').countDocuments(filter) > 0, `${name}: traces missing`);
    assert(await db.collection('changes').countDocuments(filter) > 0, `${name}: DB changes missing`);
    const timeline = await request(context.app.getHttpServer()).get(`/v1/sessions/${sessionId}/timeline`).set(auth).expect(200);
    assert(JSON.stringify(timeline.body).includes(req.rid), `${name}: viewer timeline lost request`);
    console.log(`PASS ${name}: collector -> ClickHouse -> MongoDB -> viewer API; ${policy ? 'approved policy applied, business values exact' : 'full response exact'} (${sessionId})`);
  }
  const { CaptureBudgetService } = apiRequire('./src/domain/sessions/capture-budget.service.ts');
  const usage = await context.moduleFixture.get(CaptureBudgetService).usage(owner.tenantId);
  assert(usage.bytes + usage.pendingBytes > 0, 'collector usage was not metered');
  console.log('PASS Go captures use existing metering and viewer APIs without API changes');
}

main().catch(error => { console.error(error); process.exitCode = 1; }).finally(async () => {
  for (const close of stop.reverse()) { try { await close(); } catch (error) { console.error(error); process.exitCode = 1; } }
});
