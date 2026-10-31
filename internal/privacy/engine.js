var ReproPrivacy = (() => {
  var __defProp = Object.defineProperty;
  var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
  var __getOwnPropNames = Object.getOwnPropertyNames;
  var __hasOwnProp = Object.prototype.hasOwnProperty;
  var __export = (target, all) => {
    for (var name in all)
      __defProp(target, name, { get: all[name], enumerable: true });
  };
  var __copyProps = (to, from, except, desc) => {
    if (from && typeof from === "object" || typeof from === "function") {
      for (let key of __getOwnPropNames(from))
        if (!__hasOwnProp.call(to, key) && key !== except)
          __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
    }
    return to;
  };
  var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

  // go-sdk/tools/privacy-entry.ts
  var privacy_entry_exports = {};
  __export(privacy_entry_exports, {
    apply: () => apply,
    configure: () => configure
  });

  // go-sdk/tools/privacy-crypto.ts
  function createHash(algorithm) {
    if (algorithm !== "sha256") throw new Error("Unsupported digest");
    let value = "";
    return {
      update(input) {
        value += input;
        return this;
      },
      digest(encoding) {
        if (encoding !== "hex") throw new Error("Unsupported encoding");
        return reproSHA256(value);
      }
    };
  }

  // node-sdk/src/privacy-redaction.ts
  var EMAIL_PATTERN = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi;
  var EMAIL_TOKEN_PREFIX = "email_tok_";
  var EMAIL_TOKEN_LENGTH = 12;
  function redactEmailDeterministic(value) {
    const normalized = String(value != null ? value : "").trim().toLowerCase();
    if (!normalized) {
      return `${EMAIL_TOKEN_PREFIX}unknown`;
    }
    const token = createHash("sha256").update(normalized).digest("hex").slice(0, EMAIL_TOKEN_LENGTH);
    return `${EMAIL_TOKEN_PREFIX}${token}`;
  }
  function redactEmailsInText(value) {
    if (!value) {
      return value;
    }
    return value.replace(EMAIL_PATTERN, (match) => isAlreadyMaskedEmail(match) ? match : redactEmailDeterministic(match));
  }
  function isAlreadyMaskedEmail(value) {
    if (/^email_tok_[a-z0-9]+$/i.test(value)) {
      return true;
    }
    const [local = "", domain = ""] = String(value).split("@");
    return local.includes("...") || domain.includes("...");
  }

  // node-sdk/src/privacy.ts
  var EMPTY_RUNTIME_PRIVACY_POLICY = {
    statements: [],
    rawTextHints: [],
    fingerprint: "empty"
  };
  var runtimePrivacyStatementIndexes = /* @__PURE__ */ new WeakMap();
  var comparableTokenCache = /* @__PURE__ */ new Map();
  var RUNTIME_PRIVACY_CANDIDATE_CACHE_MAX_ENTRIES = 2048;
  var RUNTIME_PRIVACY_PROVENANCE_MAX_ENTRIES = 1e3;
  var RUNTIME_PRIVACY_PROVENANCE_MAX_SERIALIZED_LENGTH = 2e5;
  var objectRuntimePrivacyProvenance = /* @__PURE__ */ new WeakMap();
  var serializedRuntimePrivacyProvenance = /* @__PURE__ */ new Map();
  var ALL_SURFACES = [
    "db.pk",
    "db.before",
    "db.after",
    "db.query",
    "db.resultMeta",
    "db.error",
    "request.headers",
    "request.body",
    "request.params",
    "request.query",
    "response.body",
    "trace.args",
    "trace.returnValue",
    "trace.error"
  ];
  var TARGET_PREFIXES = [
    "outbound.http.headers",
    "outbound.http.body",
    "log.payload",
    "request.headers",
    "request.body",
    "request.params",
    "request.query",
    "response.body",
    "trace.returnValue",
    "trace.args",
    "trace.error",
    "db.resultMeta",
    "db.document",
    "db.before",
    "db.after",
    "db.query",
    "db.error",
    "db.pk",
    "request",
    "response",
    "trace",
    "payload",
    "config",
    "env"
  ];
  function normalizeRuntimePrivacyPolicy(raw) {
    if (!raw || typeof raw !== "object") {
      return null;
    }
    const policy2 = raw;
    const flattened = flattenStatements(policy2);
    const rawTextHints = normalizeRawTextHints(policy2.rawTextHints);
    const statements = deduplicateNormalizedStatements(flattened.map((entry, index) => normalizeStatement(entry, index)).filter((entry) => entry !== null).sort((left, right) => left.priority - right.priority));
    if (!statements.length) {
      return null;
    }
    const normalizedPolicy = {
      environment: typeof policy2.environment === "string" ? policy2.environment : void 0,
      strength: policy2.strength === "weak" || policy2.strength === "medium" || policy2.strength === "strict" ? policy2.strength : void 0,
      statements,
      rawTextHints,
      fingerprint: runtimePrivacyPolicyFingerprint(
        typeof policy2.environment === "string" ? policy2.environment : void 0,
        typeof policy2.strength === "string" ? policy2.strength : void 0,
        statements,
        rawTextHints
      )
    };
    runtimePrivacyStatementIndexes.set(normalizedPolicy, buildRuntimePrivacyStatementIndex(statements));
    return normalizedPolicy;
  }
  function applyRuntimePrivacyPolicy(policy2, surface, value, context) {
    if (value === void 0) {
      return value;
    }
    return transformValue(policy2 != null ? policy2 : EMPTY_RUNTIME_PRIVACY_POLICY, surface, value, context, []);
  }
  function flattenStatements(policy2) {
    const fromGroups = Array.isArray(policy2.groups) ? policy2.groups.flatMap((group) => Array.isArray(group == null ? void 0 : group.statements) ? group.statements : []) : [];
    if (fromGroups.length) {
      return fromGroups;
    }
    return Array.isArray(policy2.statements) ? policy2.statements : [];
  }
  function normalizeStatement(input, index) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n;
    const scope = (_a = input == null ? void 0 : input.when) == null ? void 0 : _a.scope;
    const selector = typeof ((_b = input == null ? void 0 : input.when) == null ? void 0 : _b.selector) === "string" ? input.when.selector.trim() : "";
    const action = (_c = input == null ? void 0 : input.apply) == null ? void 0 : _c.action;
    if (!selector || scope !== "FIELD" && scope !== "FUNCTION" && scope !== "ROUTE" && scope !== "SCHEMA" && scope !== "QUERY" && scope !== "PATTERN" || action !== "KEEP_EXACT" && action !== "TOKENIZE" && action !== "SUMMARIZE" && action !== "DROP") {
      return null;
    }
    const pattern = typeof ((_d = input == null ? void 0 : input.when) == null ? void 0 : _d.pattern) === "string" && input.when.pattern.trim() ? safeRegExp(input.when.pattern) : void 0;
    const targetMatchers = parseTargetMatchers(typeof ((_e = input == null ? void 0 : input.when) == null ? void 0 : _e.target) === "string" ? input.when.target : "");
    const selectorTokens = selector.split("|").map((part) => part.trim()).filter(Boolean).map((part) => scope === "FIELD" ? normalizeFieldSelectorToken(part) : part).filter(Boolean);
    let selectorLeaves = selectorTokens.map((part) => {
      const normalized = part.replace(/\s*->.*$/, "").trim();
      const lastDot = normalized.lastIndexOf(".");
      return lastDot >= 0 ? normalized.slice(lastDot + 1) : normalized;
    }).filter(Boolean);
    if (scope === "FIELD" && targetMatchers.length) {
      const targetLeaves = targetMatchers.flatMap((matcher) => {
        var _a2, _b2;
        const leaf = (_b2 = (_a2 = matcher.path) == null ? void 0 : _a2[matcher.path.length - 1]) != null ? _b2 : "";
        if (!leaf || leaf === "*" || /^\d+$/.test(leaf)) {
          return [];
        }
        return [leaf];
      });
      if (targetLeaves.length) {
        selectorLeaves = Array.from(/* @__PURE__ */ new Set([...selectorLeaves, ...targetLeaves]));
      }
    }
    return {
      sid: typeof input.sid === "string" ? input.sid : `${String(scope).toLowerCase()}-${index + 1}`,
      scope,
      selector,
      action,
      priority: typeof ((_f = input == null ? void 0 : input.meta) == null ? void 0 : _f.priority) === "number" && Number.isFinite(input.meta.priority) ? input.meta.priority : index + 1,
      pattern,
      selectorTokens,
      selectorLeaves,
      targetMatchers,
      textPolicy: normalizeTextPolicy((_h = (_g = input == null ? void 0 : input.apply) == null ? void 0 : _g.textPolicy) != null ? _h : input == null ? void 0 : input.textPolicy),
      rawTextMode: normalizeRawTextMode((_j = (_i = input == null ? void 0 : input.apply) == null ? void 0 : _i.rawTextMode) != null ? _j : input == null ? void 0 : input.rawTextMode),
      rawTextHintNames: normalizeRawTextHintNames((_l = (_k = input == null ? void 0 : input.meta) == null ? void 0 : _k.review) == null ? void 0 : _l.rawTextHintNames),
      descendantRawTextMode: normalizeRawTextMode((_n = (_m = input == null ? void 0 : input.apply) == null ? void 0 : _m.descendantRawTextMode) != null ? _n : input == null ? void 0 : input.descendantRawTextMode)
    };
  }
  function deduplicateNormalizedStatements(statements) {
    const seen = /* @__PURE__ */ new Set();
    const deduplicated = [];
    for (const statement of statements) {
      const key = normalizedStatementSemanticKey(statement);
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      deduplicated.push(statement);
    }
    return deduplicated;
  }
  function normalizedStatementSemanticKey(statement) {
    var _a, _b, _c, _d;
    return JSON.stringify({
      scope: statement.scope,
      selectorTokens: statement.selectorTokens,
      selectorLeaves: statement.selectorLeaves,
      action: statement.action,
      pattern: statement.pattern ? {
        source: statement.pattern.source,
        flags: statement.pattern.flags
      } : null,
      targetMatchers: statement.targetMatchers.map((matcher) => ({
        surfaces: matcher.surfaces,
        path: matcher.path
      })),
      textPolicy: (_a = statement.textPolicy) != null ? _a : null,
      rawTextMode: (_b = statement.rawTextMode) != null ? _b : null,
      rawTextHintNames: (_c = statement.rawTextHintNames) != null ? _c : null,
      descendantRawTextMode: (_d = statement.descendantRawTextMode) != null ? _d : null
    });
  }
  function runtimePrivacyPolicyFingerprint(environment, strength, statements, rawTextHints) {
    const serialized = JSON.stringify({
      environment: environment != null ? environment : null,
      strength: strength != null ? strength : null,
      statements: statements.map(normalizedStatementSemanticKey),
      rawTextHints: rawTextHints.map((hint) => ({
        name: hint.name,
        action: hint.action,
        pattern: hint.pattern.source,
        flags: hint.pattern.flags,
        tokenLabel: hint.tokenLabel
      }))
    });
    return createHash("sha256").update(serialized).digest("hex");
  }
  function createFunctionStatementTrieNode() {
    return {
      children: /* @__PURE__ */ new Map(),
      terminal: [],
      subtree: []
    };
  }
  function appendUniqueStatement(target, statement) {
    if (!target.includes(statement)) {
      target.push(statement);
    }
  }
  function functionSelectorSegments(value) {
    return normalizeFunctionSelector(value).split(".").map((segment) => segment.trim()).filter(Boolean).reverse();
  }
  function addFunctionStatementToTrie(root, selector, statement) {
    const segments = functionSelectorSegments(selector);
    if (!segments.length) {
      return;
    }
    let node = root;
    for (const segment of segments) {
      let child = node.children.get(segment);
      if (!child) {
        child = createFunctionStatementTrieNode();
        node.children.set(segment, child);
      }
      node = child;
      appendUniqueStatement(node.subtree, statement);
    }
    appendUniqueStatement(node.terminal, statement);
  }
  function createRuntimePrivacySurfaceIndex() {
    return {
      contextualStatements: [],
      fieldStatementsBySuffix: /* @__PURE__ */ new Map(),
      fieldWildcardTargetStatements: []
    };
  }
  function appendStatementToMap(target, key, statement) {
    if (!key) {
      return;
    }
    const current = target.get(key);
    if (current) {
      appendUniqueStatement(current, statement);
      return;
    }
    target.set(key, [statement]);
  }
  function explicitTargetSurfaces(statement) {
    if (!statement.targetMatchers.length) {
      return null;
    }
    return new Set(statement.targetMatchers.flatMap((matcher) => matcher.surfaces));
  }
  function scopeCanMatchSurface(statement, surface) {
    if (statement.scope === "FUNCTION") {
      return surface.startsWith("trace.");
    }
    if (statement.scope === "ROUTE") {
      return surface.startsWith("request.") || surface === "response.body";
    }
    if (statement.scope === "SCHEMA" || statement.scope === "QUERY") {
      return surface.startsWith("db.");
    }
    return true;
  }
  function addFieldStatementToSurfaceIndex(index, statement) {
    var _a;
    for (const selector of statement.selectorLeaves) {
      appendStatementToMap(
        index.fieldStatementsBySuffix,
        normalizeComparableToken(selector),
        statement
      );
    }
    for (const matcher of statement.targetMatchers) {
      const terminal = (_a = matcher.path) == null ? void 0 : _a[matcher.path.length - 1];
      if (!terminal) {
        continue;
      }
      if (terminal === "*") {
        appendUniqueStatement(index.fieldWildcardTargetStatements, statement);
        continue;
      }
      appendStatementToMap(
        index.fieldStatementsBySuffix,
        normalizeComparableToken(terminal),
        statement
      );
    }
  }
  function buildRuntimePrivacyStatementIndex(statements) {
    var _a;
    const bySurface = new Map(
      ALL_SURFACES.map((surface) => [surface, createRuntimePrivacySurfaceIndex()])
    );
    const functionStatements = createFunctionStatementTrieNode();
    let hasNumericTargetSegments = false;
    for (const statement of statements) {
      for (const matcher of statement.targetMatchers) {
        if ((_a = matcher.path) == null ? void 0 : _a.some((segment) => /^\d+$/.test(segment))) {
          hasNumericTargetSegments = true;
        }
      }
      if (statement.scope === "FUNCTION") {
        for (const selector of statement.selectorTokens) {
          addFunctionStatementToTrie(functionStatements, selector, statement);
        }
        continue;
      }
      const targetSurfaces = explicitTargetSurfaces(statement);
      for (const surface of ALL_SURFACES) {
        if (!scopeCanMatchSurface(statement, surface)) {
          continue;
        }
        if (statement.scope !== "FIELD" && targetSurfaces && !targetSurfaces.has(surface)) {
          continue;
        }
        const surfaceIndex = bySurface.get(surface);
        if (statement.scope === "FIELD") {
          addFieldStatementToSurfaceIndex(surfaceIndex, statement);
        } else {
          appendUniqueStatement(surfaceIndex.contextualStatements, statement);
        }
      }
    }
    return {
      bySurface,
      functionStatements,
      candidateCache: /* @__PURE__ */ new Map(),
      hasNumericTargetSegments,
      scoredCandidateCache: /* @__PURE__ */ new Map()
    };
  }
  function getRuntimePrivacyStatementIndex(policy2) {
    const existing = runtimePrivacyStatementIndexes.get(policy2);
    if (existing) {
      return existing;
    }
    const created = buildRuntimePrivacyStatementIndex(policy2.statements);
    runtimePrivacyStatementIndexes.set(policy2, created);
    return created;
  }
  function matchingFunctionStatements(root, functionName) {
    const segments = functionSelectorSegments(functionName);
    if (!segments.length) {
      return [];
    }
    const matches = /* @__PURE__ */ new Set();
    let node = root;
    for (let index = 0; index < segments.length; index += 1) {
      node = node.children.get(segments[index]);
      if (!node) {
        break;
      }
      for (const statement of node.terminal) {
        matches.add(statement);
      }
      if (index === segments.length - 1) {
        for (const statement of node.subtree) {
          matches.add(statement);
        }
      }
    }
    return Array.from(matches);
  }
  function normalizedPathSuffixes(path, preserveNumericSegments) {
    const joined = path.filter(
      (segment) => segment !== "*" && (preserveNumericSegments || !/^\d+$/.test(segment))
    ).map(normalizeComparableToken).filter(Boolean).join(".");
    if (!joined) {
      return [];
    }
    const segments = joined.split(".").filter(Boolean);
    const suffixes = [];
    for (let index = 0; index < segments.length; index += 1) {
      suffixes.push(segments.slice(index).join("."));
    }
    return suffixes;
  }
  function runtimePrivacyCandidateCacheKey(index, surface, context, path) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k;
    const pathKey = path.map(
      (segment) => !index.hasNumericTargetSegments && /^\d+$/.test(segment) ? "*" : segment
    ).join(".");
    return [
      surface,
      normalizeFunctionSelector((_c = (_b = (_a = context.trace) == null ? void 0 : _a.fn) == null ? void 0 : _b.trim()) != null ? _c : ""),
      (_e = (_d = context.routeKey) == null ? void 0 : _d.trim()) != null ? _e : "",
      String((_g = (_f = context.db) == null ? void 0 : _f.type) != null ? _g : ""),
      String((_i = (_h = context.db) == null ? void 0 : _h.collection) != null ? _i : ""),
      String((_k = (_j = context.db) == null ? void 0 : _j.op) != null ? _k : ""),
      pathKey
    ].join("\0");
  }
  function candidateStatementsForMatch(policy2, surface, context, path) {
    var _a, _b, _c, _d, _e;
    const index = getRuntimePrivacyStatementIndex(policy2);
    const cacheKey = runtimePrivacyCandidateCacheKey(index, surface, context, path);
    const cached = index.candidateCache.get(cacheKey);
    if (cached) {
      return cached;
    }
    const surfaceIndex = index.bySurface.get(surface);
    const candidateSet = new Set(
      (_a = surfaceIndex == null ? void 0 : surfaceIndex.contextualStatements) != null ? _a : []
    );
    if (surfaceIndex && path.length) {
      for (const suffix of normalizedPathSuffixes(path, index.hasNumericTargetSegments)) {
        for (const statement of (_b = surfaceIndex.fieldStatementsBySuffix.get(suffix)) != null ? _b : []) {
          candidateSet.add(statement);
        }
      }
      for (const statement of surfaceIndex.fieldWildcardTargetStatements) {
        candidateSet.add(statement);
      }
    }
    if (surface.startsWith("trace.")) {
      const functionName = (_e = (_d = (_c = context.trace) == null ? void 0 : _c.fn) == null ? void 0 : _d.trim()) != null ? _e : "";
      if (functionName) {
        for (const statement of matchingFunctionStatements(index.functionStatements, functionName)) {
          candidateSet.add(statement);
        }
      }
    }
    const candidates = Array.from(candidateSet).sort((left, right) => left.priority - right.priority);
    if (index.candidateCache.size >= RUNTIME_PRIVACY_CANDIDATE_CACHE_MAX_ENTRIES) {
      index.candidateCache.clear();
    }
    index.candidateCache.set(cacheKey, candidates);
    return candidates;
  }
  function resolveEffectiveAction(baseAction, value, path) {
    const safetyFloor = deriveSensitiveLeafFloorAction(value, path);
    if (!safetyFloor) {
      return baseAction;
    }
    if (!baseAction) {
      return safetyFloor;
    }
    if (baseAction === "KEEP_EXACT") {
      return safetyFloor === "SUMMARIZE" ? baseAction : safetyFloor;
    }
    if (baseAction === "DROP") {
      return baseAction;
    }
    if (safetyFloor === "DROP") {
      return "DROP";
    }
    if (safetyFloor === "TOKENIZE" && baseAction === "SUMMARIZE") {
      return "TOKENIZE";
    }
    return baseAction;
  }
  function deriveSensitiveLeafFloorAction(value, path) {
    if (!path.length) {
      return null;
    }
    if (value !== null && typeof value === "object") {
      return null;
    }
    const comparablePath = normalizeComparablePath(path);
    if (!comparablePath) {
      return null;
    }
    if (DROP_SAFETY_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return "DROP";
    }
    if (TOKENIZE_SAFETY_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return "TOKENIZE";
    }
    if (typeof value === "string") {
      if (looksLikeEmail(value)) {
        return "TOKENIZE";
      }
      if (looksLikeConnectionString(value)) {
        return "DROP";
      }
      if (FREE_TEXT_FALLBACK_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
        return "SUMMARIZE";
      }
    }
    return null;
  }
  function transformValue(policy2, surface, value, context, path, fallbackInput = null) {
    var _a, _b;
    const fallback = resolveTransformFallback(value, fallbackInput);
    const objectId = coercePrivacyObjectId(value);
    if (objectId !== null) {
      value = objectId;
    }
    value = normalizeEncodedCollectionForPrivacy(value);
    const statement = matchStatement(policy2, surface, value, context, path);
    if (((_a = statement == null ? void 0 : statement.textPolicy) == null ? void 0 : _a.mode) === "KNOWN_TEMPLATE" && typeof value === "string") {
      return applyKnownTextPolicy(statement.textPolicy, value, context, surface, path);
    }
    let currentValue = value;
    const action = resolveEffectiveAction((_b = statement == null ? void 0 : statement.action) != null ? _b : fallback == null ? void 0 : fallback.action, currentValue, path);
    const deferKnownStringDetectors = shouldDeferKnownStringDetectorsToRawHints(statement, fallback);
    if (typeof currentValue === "string" && action !== "DROP" && action !== "TOKENIZE") {
      const structured = transformSerializedStructuredString(
        policy2,
        surface,
        currentValue,
        context,
        path,
        serializedStringFallbackForStatement(statement, action, fallback)
      );
      if (structured !== null) {
        return structured;
      }
      if (!deferKnownStringDetectors) {
        const detected = sanitizeTextWithKnownDetectorsSync(currentValue, path);
        if (detected !== currentValue && (action === "KEEP_EXACT" || !action) && !(statement == null ? void 0 : statement.descendantRawTextMode)) {
          return detected;
        }
        currentValue = detected;
      }
    }
    if (typeof currentValue === "string" && (action === "DROP" || action === "TOKENIZE")) {
      const structured = transformSerializedStructuredStringAsChildren(policy2, surface, currentValue, context, path);
      if (structured !== null) {
        return structured;
      }
    }
    if (shouldPreserveBroadContainerSurface(statement, action, surface, currentValue, path)) {
      return transformChildrenExact(policy2, surface, currentValue, context, path, childFallbackForExact(statement, action, fallback));
    }
    if (typeof currentValue === "string" && action === "KEEP_EXACT" && (statement == null ? void 0 : statement.descendantRawTextMode)) {
      return summarizeValue(policy2, currentValue, context, surface, path, { rawTextMode: statement.descendantRawTextMode });
    }
    if (action === "KEEP_EXACT") {
      return transformChildrenExact(policy2, surface, currentValue, context, path, childFallbackForExact(statement, action, fallback));
    }
    if (action === "DROP") {
      return "[dropped]";
    }
    if (action === "TOKENIZE") {
      if (isStructuredPrivacyContainer(currentValue)) {
        return transformChildrenWithFallback(policy2, surface, currentValue, context, path, "TOKENIZE");
      }
      return tokenizeValue(currentValue, context, surface, path);
    }
    if (action === "SUMMARIZE") {
      const summaryPolicy = summaryPolicyFor(statement, fallback);
      const summaryFallback = summaryFallbackForPolicy(summaryPolicy);
      if (isStructuredPrivacyContainer(currentValue) && summaryFallback) {
        return transformChildrenExact(policy2, surface, currentValue, context, path, summaryFallback);
      }
      if (!statement && (fallback == null ? void 0 : fallback.action) === "SUMMARIZE" && isStructuredPrivacyContainer(currentValue)) {
        return transformChildrenExact(policy2, surface, currentValue, context, path, fallback);
      }
      return summarizeValue(policy2, currentValue, context, surface, path, summaryPolicy);
    }
    return transformChildrenExact(policy2, surface, currentValue, context, path, fallback);
  }
  function resolveTransformFallback(value, input) {
    const explicit = normalizeTransformFallback(input);
    if (explicit) {
      rememberRuntimePrivacyProvenance(value, explicit);
      return explicit;
    }
    return getRuntimePrivacyProvenance(value);
  }
  function normalizeTransformFallback(input) {
    if (!input) {
      return null;
    }
    if (typeof input === "string") {
      return { action: input };
    }
    return input;
  }
  function childFallbackForExact(statement, action, inherited) {
    if (statement) {
      if (action === "KEEP_EXACT" && statement.descendantRawTextMode) {
        const mergedRawTextMode = mergeRawTextMode(statement.descendantRawTextMode, inherited == null ? void 0 : inherited.rawTextMode);
        const inheritedWins = inherited !== null && inherited.rawTextMode === mergedRawTextMode && inherited.rawTextMode !== statement.descendantRawTextMode;
        return {
          action: "SUMMARIZE",
          rawTextMode: mergedRawTextMode,
          rawTextHintNames: inheritedWins ? inherited.rawTextHintNames : statement.rawTextHintNames,
          sourceSid: inheritedWins ? inherited.sourceSid : statement.sid,
          sourceSelector: inheritedWins ? inherited.sourceSelector : statement.selector
        };
      }
      return action === "KEEP_EXACT" ? inherited : null;
    }
    return inherited;
  }
  function summaryPolicyFor(statement, fallback) {
    if (statement) {
      if (statement.textPolicy) {
        return statement;
      }
      const rawTextMode = mergeRawTextMode(statement.rawTextMode, fallback == null ? void 0 : fallback.rawTextMode);
      return rawTextMode ? { ...statement, rawTextMode } : statement;
    }
    return (fallback == null ? void 0 : fallback.rawTextMode) ? { rawTextMode: fallback.rawTextMode, rawTextHintNames: fallback.rawTextHintNames } : null;
  }
  function shouldDeferKnownStringDetectorsToRawHints(statement, fallback) {
    return mergeRawTextMode(statement == null ? void 0 : statement.rawTextMode, fallback == null ? void 0 : fallback.rawTextMode) === "REGEX_ASSISTED_EXACT";
  }
  function mergeRawTextMode(primary, inherited) {
    if (primary === "REGEX_ASSISTED_EXACT" || inherited === "REGEX_ASSISTED_EXACT") {
      return "REGEX_ASSISTED_EXACT";
    }
    return primary != null ? primary : inherited;
  }
  function summaryFallbackForPolicy(policy2) {
    return (policy2 == null ? void 0 : policy2.rawTextMode) ? { action: "SUMMARIZE", rawTextMode: policy2.rawTextMode, rawTextHintNames: policy2.rawTextHintNames } : null;
  }
  function serializedStringFallbackForStatement(statement, action, inherited) {
    if (!statement || action === "DROP" || action === "TOKENIZE") {
      return inherited;
    }
    const rawTextMode = mergeRawTextMode(statement.rawTextMode, inherited == null ? void 0 : inherited.rawTextMode);
    if (!rawTextMode) {
      return inherited;
    }
    const inheritedWins = inherited !== null && inherited.rawTextMode === rawTextMode && inherited.rawTextMode !== statement.rawTextMode;
    return {
      action: "SUMMARIZE",
      rawTextMode,
      rawTextHintNames: inheritedWins ? inherited.rawTextHintNames : statement.rawTextHintNames,
      sourceSid: inheritedWins ? inherited.sourceSid : statement.sid,
      sourceSelector: inheritedWins ? inherited.sourceSelector : statement.selector
    };
  }
  function isRuntimePrivacyProvenanceFallback(fallback) {
    return Boolean(fallback && fallback.action === "SUMMARIZE" && fallback.rawTextMode);
  }
  function rememberRuntimePrivacyProvenance(value, fallback) {
    if (!isRuntimePrivacyProvenanceFallback(fallback)) {
      return;
    }
    const provenance = {
      action: fallback.action,
      rawTextMode: fallback.rawTextMode,
      sourceSid: fallback.sourceSid,
      sourceSelector: fallback.sourceSelector,
      inherited: true
    };
    if (value && typeof value === "object") {
      objectRuntimePrivacyProvenance.set(value, provenance);
    }
    rememberSerializedRuntimePrivacyProvenance(value, provenance);
  }
  function getRuntimePrivacyProvenance(value) {
    if (value && typeof value === "object") {
      const objectProvenance = objectRuntimePrivacyProvenance.get(value);
      if (objectProvenance) {
        return objectProvenance;
      }
    }
    return lookupSerializedRuntimePrivacyProvenance(value);
  }
  function rememberSerializedRuntimePrivacyProvenance(value, provenance) {
    for (const serialized of runtimePrivacyProvenanceSerializations(value)) {
      const key = hashRuntimePrivacyProvenance(serialized);
      if (!serializedRuntimePrivacyProvenance.has(key) && serializedRuntimePrivacyProvenance.size >= RUNTIME_PRIVACY_PROVENANCE_MAX_ENTRIES) {
        const first = serializedRuntimePrivacyProvenance.keys().next().value;
        if (first) {
          serializedRuntimePrivacyProvenance.delete(first);
        }
      }
      serializedRuntimePrivacyProvenance.set(key, provenance);
    }
  }
  function lookupSerializedRuntimePrivacyProvenance(value) {
    if (serializedRuntimePrivacyProvenance.size === 0) return null;
    for (const serialized of runtimePrivacyProvenanceSerializations(value)) {
      const provenance = serializedRuntimePrivacyProvenance.get(hashRuntimePrivacyProvenance(serialized));
      if (provenance) {
        return provenance;
      }
    }
    return null;
  }
  function runtimePrivacyProvenanceSerializations(value) {
    if (value === void 0 || value === null) {
      return [];
    }
    if (typeof value === "string") {
      if (!value || value.length > RUNTIME_PRIVACY_PROVENANCE_MAX_SERIALIZED_LENGTH) {
        return [];
      }
      const out2 = [value];
      const parsed = parseSerializedStructuredText(value);
      if (parsed !== null) {
        const stable2 = stableStringifyForRuntimePrivacyProvenance(parsed);
        if (stable2 && stable2 !== value) {
          out2.push(stable2);
        }
      }
      return out2;
    }
    if (!isStructuredPrivacyContainer(value)) {
      return [];
    }
    const out = [];
    const json = jsonStringifyForRuntimePrivacyProvenance(value);
    if (json) {
      out.push(json);
    }
    const stable = stableStringifyForRuntimePrivacyProvenance(value);
    if (stable && stable !== json) {
      out.push(stable);
    }
    return out;
  }
  function jsonStringifyForRuntimePrivacyProvenance(value) {
    try {
      const serialized = JSON.stringify(value);
      return serialized && serialized.length <= RUNTIME_PRIVACY_PROVENANCE_MAX_SERIALIZED_LENGTH ? serialized : null;
    } catch (e) {
      return null;
    }
  }
  function stableStringifyForRuntimePrivacyProvenance(value) {
    try {
      const seen = /* @__PURE__ */ new WeakSet();
      const serialized = stableStringifyForRuntimePrivacyProvenanceInner(value, seen);
      return serialized && serialized.length <= RUNTIME_PRIVACY_PROVENANCE_MAX_SERIALIZED_LENGTH ? serialized : null;
    } catch (e) {
      return null;
    }
  }
  function stableStringifyForRuntimePrivacyProvenanceInner(value, seen) {
    if (value === void 0) return "undefined";
    if (value === null) return "null";
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (typeof value === "bigint") {
      return `${value.toString()}n`;
    }
    if (typeof value !== "object") {
      return String(value);
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (value instanceof Error || isWeakCollection(value)) {
      return null;
    }
    if (seen.has(value)) {
      return null;
    }
    seen.add(value);
    try {
      if (Array.isArray(value)) {
        const items = value.map((item) => stableStringifyForRuntimePrivacyProvenanceInner(item, seen));
        if (items.some((item) => item === null)) {
          return null;
        }
        return `[${items.join(",")}]`;
      }
      if (value instanceof Map) {
        const entries = Array.from(value.entries()).map(([key, child], index) => [
          normalizeCollectionKeyForPrivacy(key, index),
          stableStringifyForRuntimePrivacyProvenanceInner(child, seen)
        ]);
        if (entries.some(([, child]) => child === null)) {
          return null;
        }
        return `{${entries.sort(([left], [right]) => left.localeCompare(right)).map(([key, child]) => `${key}:${child}`).join(",")}}`;
      }
      if (value instanceof Set) {
        const items = Array.from(value.values()).map((item) => stableStringifyForRuntimePrivacyProvenanceInner(item, seen));
        if (items.some((item) => item === null)) {
          return null;
        }
        return `[${items.join(",")}]`;
      }
      const keys = Object.keys(value).sort();
      const fields = [];
      for (const key of keys) {
        const child = stableStringifyForRuntimePrivacyProvenanceInner(value[key], seen);
        if (child === null) {
          return null;
        }
        fields.push(`${key}:${child}`);
      }
      return `{${fields.join(",")}}`;
    } finally {
      seen.delete(value);
    }
  }
  function hashRuntimePrivacyProvenance(serialized) {
    return createHash("sha256").update(`runtime-privacy-provenance|${serialized}`).digest("hex");
  }
  function transformChildrenExact(policy2, surface, value, context, path, fallback = null) {
    rememberRuntimePrivacyProvenance(value, fallback);
    if (Array.isArray(value)) {
      return value.map((item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), fallback));
    }
    if (value instanceof Map) {
      const output = {};
      let index = 0;
      for (const [key, child] of value.entries()) {
        const normalizedKey = normalizeCollectionKeyForPrivacy(key, index);
        output[normalizedKey] = transformValue(policy2, surface, child, context, path.concat(normalizedKey), fallback);
        index += 1;
      }
      return output;
    }
    if (value instanceof Set) {
      return Array.from(value.values()).map(
        (item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), fallback)
      );
    }
    if (isWeakCollection(value)) {
      return `[${getPrivacyConstructorName(value)}: uninspectable]`;
    }
    if (value && typeof value === "object") {
      if (value instanceof Date || value instanceof Error) {
        return value;
      }
      const output = {};
      for (const [key, child] of Object.entries(value)) {
        output[key] = transformValue(policy2, surface, child, context, path.concat(key), fallback);
      }
      return output;
    }
    return value;
  }
  function transformChildrenWithFallback(policy2, surface, value, context, path, fallbackAction) {
    if (Array.isArray(value)) {
      return value.map((item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), fallbackAction));
    }
    if (value instanceof Map) {
      const output = {};
      let index = 0;
      for (const [key, child] of value.entries()) {
        const normalizedKey = normalizeCollectionKeyForPrivacy(key, index);
        output[normalizedKey] = transformValue(policy2, surface, child, context, path.concat(normalizedKey), fallbackAction);
        index += 1;
      }
      return output;
    }
    if (value instanceof Set) {
      return Array.from(value.values()).map(
        (item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), fallbackAction)
      );
    }
    if (isWeakCollection(value)) {
      return transformValue(policy2, surface, `[${getPrivacyConstructorName(value)}: uninspectable]`, context, path, fallbackAction);
    }
    if (value && typeof value === "object") {
      if (value instanceof Date || value instanceof Error) {
        return transformValue(policy2, surface, String(value), context, path, fallbackAction);
      }
      const output = {};
      for (const [key, child] of Object.entries(value)) {
        output[key] = transformValue(policy2, surface, child, context, path.concat(key), fallbackAction);
      }
      return output;
    }
    return transformValue(policy2, surface, value, context, path, fallbackAction);
  }
  function isStructuredPrivacyContainer(value) {
    return value !== null && typeof value === "object" && !(value instanceof Date) && !(value instanceof Error);
  }
  function normalizeCollectionKeyForPrivacy(value, index) {
    const objectId = coercePrivacyObjectId(value);
    if (objectId !== null) {
      return objectId;
    }
    if (typeof value === "string" && value) {
      return value;
    }
    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
      return String(value);
    }
    if (typeof value === "symbol") {
      return value.description ? `symbol:${value.description}` : `symbol:${index}`;
    }
    return `mapEntry${index}`;
  }
  function isWeakCollection(value) {
    return value instanceof WeakMap || value instanceof WeakSet;
  }
  function getPrivacyConstructorName(value) {
    var _a;
    return ((_a = value == null ? void 0 : value.constructor) == null ? void 0 : _a.name) || "Object";
  }
  function normalizeEncodedCollectionForPrivacy(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return value;
    }
    const type = typeof value.__type === "string" ? value.__type : "";
    if (type === "Map" && Array.isArray(value.entries)) {
      const output = {};
      value.entries.forEach((entry, index) => {
        if (!Array.isArray(entry) || !entry.length) {
          return;
        }
        output[normalizeCollectionKeyForPrivacy(entry[0], index)] = entry[1];
      });
      return output;
    }
    if (type === "Set" && Array.isArray(value.values)) {
      return value.values;
    }
    return value;
  }
  function transformSerializedStructuredString(policy2, surface, value, context, path, fallback = null) {
    const parsed = parseSerializedStructuredText(value);
    if (parsed === null) {
      return null;
    }
    const transformed = transformValue(policy2, surface, parsed, context, path, fallback);
    if (stableStringify(transformed) === stableStringify(parsed)) {
      return null;
    }
    return JSON.stringify(transformed);
  }
  function transformSerializedStructuredStringAsChildren(policy2, surface, value, context, path) {
    const parsed = parseSerializedStructuredText(value);
    if (parsed === null) {
      return null;
    }
    const transformed = transformChildrenExact(policy2, surface, parsed, context, path);
    if (stableStringify(transformed) === stableStringify(parsed)) {
      return null;
    }
    return JSON.stringify(transformed);
  }
  function matchStatement(policy2, surface, value, context, path) {
    var _a, _b, _c, _d, _e;
    const index = getRuntimePrivacyStatementIndex(policy2);
    const key = JSON.stringify([
      surface,
      (_a = context.trace) == null ? void 0 : _a.fn,
      context.routeKey,
      (_b = context.db) == null ? void 0 : _b.type,
      (_c = context.db) == null ? void 0 : _c.collection,
      (_d = context.db) == null ? void 0 : _d.op,
      path
    ]);
    let scored = index.scoredCandidateCache.get(key);
    if (!scored) {
      scored = scoreStatementCandidates(policy2, surface, context, path);
      if (index.scoredCandidateCache.size >= RUNTIME_PRIVACY_CANDIDATE_CACHE_MAX_ENTRIES) {
        index.scoredCandidateCache.clear();
      }
      index.scoredCandidateCache.set(key, scored);
    }
    const leaf = (_e = path[path.length - 1]) != null ? _e : "";
    const joinedPath = normalizeComparablePath(path);
    const candidates = scored.filter(({ statement }) => statement.scope !== "PATTERN" || patternMatches(statement.pattern, leaf, joinedPath, value));
    return selectStatementCandidate(candidates, surface, value, path);
  }
  function scoreStatementCandidates(policy2, surface, context, path) {
    let candidates = [];
    for (const statement of candidateStatementsForMatch(policy2, surface, context, path)) {
      if (shouldIgnoreBroadContainerRuleOnDescendant(statement, surface, path)) {
        continue;
      }
      let targetScore = bestTargetMatchScore(statement.targetMatchers, surface, path);
      if (targetScore < 0 && statement.scope === "FIELD") {
        targetScore = relaxedFieldTargetMatchScore(statement, surface, path);
      }
      if (targetScore < 0) {
        continue;
      }
      if (statement.scope !== "PATTERN" && !matchesScope(statement, surface, void 0, context, path)) {
        continue;
      }
      const selectorScore = matchSelectorSpecificity(statement, surface, context, path);
      candidates.push({
        statement,
        targetScore,
        score: targetScore * 1e3 + selectorScore + semanticScopeSpecificity(statement) + statementSpecificity(statement) - statement.priority
      });
    }
    return candidates.filter(({ statement }) => !isBroadContextKeepExact(statement, surface, path));
  }
  function selectStatementCandidate(candidates, surface, value, path) {
    var _a;
    if (!candidates.length) {
      return null;
    }
    const textPolicyCandidates = typeof value === "string" ? candidates.filter(({ statement }) => {
      var _a2;
      return ((_a2 = statement.textPolicy) == null ? void 0 : _a2.mode) === "KNOWN_TEMPLATE";
    }) : [];
    if (textPolicyCandidates.length) {
      candidates.length = 0;
      candidates.push(...textPolicyCandidates);
    }
    const strongestProtectiveTargetScore = Math.max(
      -1,
      ...candidates.filter(({ statement }) => statement.action !== "KEEP_EXACT").map(({ targetScore }) => targetScore)
    );
    if (strongestProtectiveTargetScore >= 0) {
      candidates = candidates.filter(
        ({ statement, targetScore }) => statement.action !== "KEEP_EXACT" || targetScore > strongestProtectiveTargetScore
      );
    }
    const hasNestedTransform = path.length > 0 && candidates.some(
      ({ statement, targetScore }) => statement.action !== "KEEP_EXACT" && (targetScore > 0 || statement.scope === "FIELD" || statement.scope === "PATTERN")
    );
    const active = hasNestedTransform ? candidates.filter((candidate) => !isBroadContextKeepExact(candidate.statement, surface, path)) : candidates;
    const pool = active.length ? active : candidates;
    let best = null;
    for (const candidate of pool) {
      if (!best || candidate.score > best.score) {
        best = { statement: candidate.statement, score: candidate.score };
      }
    }
    return (_a = best == null ? void 0 : best.statement) != null ? _a : null;
  }
  function semanticScopeSpecificity(statement) {
    switch (statement.scope) {
      case "FIELD":
        return 350;
      case "PATTERN":
        return 325;
      case "SCHEMA":
      case "QUERY":
        return 250;
      default:
        return 0;
    }
  }
  function actionSpecificity(action) {
    switch (action) {
      case "DROP":
        return 300;
      case "TOKENIZE":
        return 200;
      case "SUMMARIZE":
        return 100;
      case "KEEP_EXACT":
      default:
        return 0;
    }
  }
  function statementSpecificity(statement) {
    var _a;
    let score = actionSpecificity(statement.action);
    if (((_a = statement.textPolicy) == null ? void 0 : _a.mode) === "KNOWN_TEMPLATE") {
      score += 90;
    }
    score += rawTextModeSpecificity(statement.rawTextMode);
    score += rawTextModeSpecificity(statement.descendantRawTextMode);
    return score;
  }
  function rawTextModeSpecificity(mode) {
    if (mode === "REGEX_ASSISTED_EXACT") {
      return 80;
    }
    if (mode === "SAFE_PARTIAL_MASK") {
      return 10;
    }
    return 0;
  }
  function shouldPreserveBroadContainerSurface(statement, action, surface, value, path) {
    if (!statement || !action || action === "KEEP_EXACT") {
      return false;
    }
    if (action === "TOKENIZE") {
      return false;
    }
    if (value === null || value === void 0 || typeof value !== "object" || value instanceof Date || value instanceof Error) {
      return false;
    }
    if (!hasStructuredContainerTargetMatch(statement.targetMatchers, surface, path)) {
      return false;
    }
    return statement.scope === "FUNCTION" && surface.startsWith("trace.") || statement.scope === "ROUTE" && (surface.startsWith("request.") || surface === "response.body") || (statement.scope === "QUERY" || statement.scope === "SCHEMA") && surface.startsWith("db.");
  }
  function shouldIgnoreBroadContainerRuleOnDescendant(statement, surface, path) {
    if (!path.length || statement.action === "KEEP_EXACT") {
      return false;
    }
    if (!hasStructuredContainerAncestorTarget(statement.targetMatchers, surface, path)) {
      return false;
    }
    return statement.scope === "FUNCTION" && surface.startsWith("trace.") || statement.scope === "ROUTE" && (surface.startsWith("request.") || surface === "response.body") || (statement.scope === "QUERY" || statement.scope === "SCHEMA") && surface.startsWith("db.");
  }
  function matchSelectorSpecificity(statement, surface, context, path) {
    var _a, _b, _c;
    switch (statement.scope) {
      case "FUNCTION": {
        const fn = (_b = (_a = context.trace) == null ? void 0 : _a.fn) == null ? void 0 : _b.trim();
        if (!fn) {
          return 0;
        }
        const normalizedFn = normalizeFunctionSelector(fn);
        let best = 0;
        for (const selector of statement.selectorTokens) {
          if (!functionSelectorMatches(selector, fn)) {
            continue;
          }
          const normalizedSelector = normalizeFunctionSelector(selector);
          const dotCount = normalizedSelector.split(".").filter(Boolean).length;
          const score = 400 + dotCount * 25 - (statement.selectorTokens.length - 1) * 50;
          best = Math.max(best, score);
        }
        return best || (normalizedFn ? 1 : 0);
      }
      case "FIELD": {
        const leaf = (_c = path[path.length - 1]) != null ? _c : "";
        const joinedPath = normalizeComparablePath(path);
        let best = 0;
        for (const selector of statement.selectorLeaves) {
          if (!tokenMatchesPathToken(selector, leaf, joinedPath)) {
            continue;
          }
          const normalizedSelector = normalizeComparableToken(selector);
          const dotCount = normalizedSelector.split(".").filter(Boolean).length;
          best = Math.max(best, 150 + dotCount * 15 - (statement.selectorLeaves.length - 1) * 10);
        }
        return best;
      }
      case "ROUTE":
        return 200 - (statement.selectorTokens.length - 1) * 25;
      default:
        return 0;
    }
  }
  function isBroadContextKeepExact(statement, surface, path) {
    var _a;
    if (statement.action !== "KEEP_EXACT") {
      return false;
    }
    if (((_a = statement.textPolicy) == null ? void 0 : _a.mode) === "KNOWN_TEMPLATE") {
      return false;
    }
    if (!path.length) {
      return false;
    }
    if (hasSpecificExactTargetMatch(statement.targetMatchers, surface, path)) {
      return false;
    }
    return statement.scope === "FIELD" || statement.scope === "FUNCTION" || statement.scope === "ROUTE" || statement.scope === "SCHEMA" || statement.scope === "QUERY";
  }
  function hasSpecificExactTargetMatch(matchers, surface, path) {
    return matchers.some((matcher) => {
      if (!matcher.surfaces.includes(surface) || !matcher.path || matcher.path.length !== path.length) {
        return false;
      }
      return matcher.path.every((segment, index) => segment === path[index]);
    });
  }
  function matchesScope(statement, surface, value, context, path) {
    var _a, _b, _c;
    const leaf = (_a = path[path.length - 1]) != null ? _a : "";
    const joinedPath = normalizeComparablePath(path);
    switch (statement.scope) {
      case "FIELD":
        return matchesFieldScope(statement, surface, path, leaf, joinedPath);
      case "FUNCTION": {
        if (!surface.startsWith("trace.")) return false;
        const fn = (_c = (_b = context.trace) == null ? void 0 : _b.fn) == null ? void 0 : _c.trim();
        if (!fn) return false;
        return statement.selectorTokens.some((selector) => functionSelectorMatches(selector, fn));
      }
      case "ROUTE":
        return Boolean(
          context.routeKey && statement.selectorTokens.some((selector) => routeSelectorMatches(selector, context.routeKey))
        );
      case "SCHEMA":
        return schemaSelectorMatches(statement.selectorTokens, surface, context, leaf, joinedPath);
      case "QUERY":
        return querySelectorMatches(statement.selectorTokens, surface, context);
      case "PATTERN":
        return patternMatches(statement.pattern, leaf, joinedPath, value);
      default:
        return false;
    }
  }
  function matchesFieldScope(statement, surface, path, leaf, joinedPath) {
    const selectorMatches = statement.selectorLeaves.some((token) => tokenMatchesPathToken(token, leaf, joinedPath));
    if (!statement.targetMatchers.length) {
      return selectorMatches;
    }
    const hasSpecificTargetMatch = statement.targetMatchers.some((matcher) => {
      if (!matcher.surfaces.includes(surface) || !matcher.path || !matcher.path.length) {
        return false;
      }
      return pathPatternMatches(matcher.path, path);
    });
    if (hasSpecificTargetMatch) {
      return true;
    }
    return selectorMatches;
  }
  function hasStructuredContainerTargetMatch(matchers, surface, path) {
    return matchers.some((matcher) => {
      if (!matcher.surfaces.includes(surface)) {
        return false;
      }
      if (!matcher.path || !matcher.path.length) {
        return path.length === 0;
      }
      if (matcher.path.length !== path.length) {
        return false;
      }
      return matcher.path.every((segment, index) => segment === path[index]);
    });
  }
  function hasStructuredContainerAncestorTarget(matchers, surface, path) {
    return matchers.some((matcher) => {
      if (!matcher.surfaces.includes(surface)) {
        return false;
      }
      if (!matcher.path || !matcher.path.length) {
        return true;
      }
      if (matcher.path.length >= path.length) {
        return false;
      }
      return matcher.path.every((segment, index) => segment === path[index]);
    });
  }
  function schemaSelectorMatches(selectors, surface, context, leaf, joinedPath) {
    var _a, _b;
    if (surface.startsWith("db.")) {
      const collection = String((_b = (_a = context.db) == null ? void 0 : _a.collection) != null ? _b : "").toLowerCase();
      if (collection && selectors.some((selector) => normalizeSchemaSelector(selector).toLowerCase() === collection)) {
        return true;
      }
    }
    return selectors.some((selector) => {
      const normalized = normalizeSchemaSelector(selector);
      if (tokenMatchesPathToken(normalized, leaf, joinedPath)) {
        return true;
      }
      const fieldToken = normalized.split(/[.\s/]+/).map((part) => part.trim()).filter(Boolean).pop();
      return fieldToken ? tokenMatchesPathToken(fieldToken, leaf, joinedPath) : false;
    });
  }
  function querySelectorMatches(selectors, surface, context) {
    var _a, _b, _c, _d, _e, _f, _g, _h;
    if (!surface.startsWith("db.")) {
      return false;
    }
    const haystack = [
      String((_b = (_a = context.db) == null ? void 0 : _a.type) != null ? _b : ""),
      String((_d = (_c = context.db) == null ? void 0 : _c.collection) != null ? _d : ""),
      String((_f = (_e = context.db) == null ? void 0 : _e.op) != null ? _f : ""),
      String((_h = (_g = context.trace) == null ? void 0 : _g.fn) != null ? _h : "")
    ].join(" ").toLowerCase();
    return selectors.some((selector) => {
      const normalized = normalizeQuerySelector(selector).trim().toLowerCase();
      return normalized && haystack.includes(normalized);
    });
  }
  function patternMatches(pattern, leaf, joinedPath, value) {
    if (!pattern) return false;
    if (leaf && testRuntimePrivacyPattern(pattern, leaf)) return true;
    if (joinedPath && testRuntimePrivacyPattern(pattern, joinedPath)) return true;
    if (typeof value === "string" && testRuntimePrivacyPattern(pattern, value)) return true;
    return false;
  }
  function testRuntimePrivacyPattern(pattern, value) {
    pattern.lastIndex = 0;
    try {
      return pattern.test(value);
    } finally {
      pattern.lastIndex = 0;
    }
  }
  function functionSelectorMatches(selector, fn) {
    const normalizedSelector = normalizeFunctionSelector(selector);
    const normalizedFn = normalizeFunctionSelector(fn);
    if (!normalizedSelector || !normalizedFn) {
      return false;
    }
    return normalizedFn === normalizedSelector || normalizedFn.endsWith(`.${normalizedSelector}`) || normalizedSelector.endsWith(`.${normalizedFn}`);
  }
  function normalizeFunctionSelector(value) {
    return value.trim().replace(/^fn:/i, "").replace(/^function[.:]/i, "").replace(/^method[.:]/i, "").replace(/(\.args.*|\.returnValue.*|\.return.*|\.error.*|\.log.*)$/i, "").trim();
  }
  function normalizeRouteSelector(value) {
    return value.trim().replace(/^route:/i, "").trim();
  }
  function routeSelectorMatches(selector, routeKey) {
    const normalizedSelector = normalizeRouteSelector(selector);
    const normalizedRouteKey = routeKey.trim();
    if (!normalizedSelector || !normalizedRouteKey) {
      return false;
    }
    if (normalizedSelector === normalizedRouteKey) {
      return true;
    }
    const escaped = normalizedSelector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/:[A-Za-z_][A-Za-z0-9_]*/g, "[^\\s/]+");
    try {
      return new RegExp(`^${escaped}$`).test(normalizedRouteKey);
    } catch (e) {
      return false;
    }
  }
  function normalizeSchemaSelector(value) {
    return value.trim().replace(/^schema:/i, "").trim();
  }
  function normalizeQuerySelector(value) {
    return value.trim().replace(/^query:/i, "").trim();
  }
  function normalizeFieldSelectorToken(value) {
    return value.trim().replace(/^field\s*:/i, "").replace(/^leaf\s*:/i, "").replace(/^path\s*:/i, "").replace(/^key\s*:/i, "").replace(/^property\s*:/i, "").replace(/^prop\s*:/i, "").trim();
  }
  function tokenMatchesPathToken(token, leaf, joinedPath) {
    const normalizedToken = normalizeComparableToken(token);
    if (!normalizedToken) return false;
    if (normalizeComparableToken(leaf) === normalizedToken) return true;
    return joinedPath === normalizedToken || joinedPath.endsWith(`.${normalizedToken}`);
  }
  function normalizeComparablePath(path) {
    return path.filter((segment) => segment !== "*" && !/^\d+$/.test(segment)).map(normalizeComparableToken).filter(Boolean).join(".");
  }
  function normalizeComparableToken(value) {
    const cached = comparableTokenCache.get(value);
    if (cached !== void 0) return cached;
    const normalized = value.trim().replace(/\s+/g, "").replace(/\[\*\]/g, "").replace(/\[\d+\]/g, "").replace(/^\$\.?/, "").replace(/^.*->/, "").replace(/\|/g, "").toLowerCase();
    if (value.length <= 1024) {
      if (comparableTokenCache.size >= 2048) comparableTokenCache.clear();
      comparableTokenCache.set(value, normalized);
    }
    return normalized;
  }
  function relaxedFieldTargetMatchScore(statement, surface, path) {
    var _a;
    if (!path.length) {
      return -1;
    }
    const leaf = (_a = path[path.length - 1]) != null ? _a : "";
    const joinedPath = normalizeComparablePath(path);
    const leafMatches = statement.selectorLeaves.some((token) => tokenMatchesPathToken(token, leaf, joinedPath));
    if (!leafMatches) {
      return -1;
    }
    if (statement.targetMatchers.length && !statement.targetMatchers.some((matcher) => matcher.surfaces.includes(surface))) {
      return statement.action !== "KEEP_EXACT" || statement.textPolicy ? 15 : -1;
    }
    let best = 25;
    for (const matcher of statement.targetMatchers) {
      if (!matcher.surfaces.includes(surface)) {
        continue;
      }
      best = Math.max(best, relaxedSuffixSpecificity(matcher.path, path));
    }
    return best;
  }
  function relaxedSuffixSpecificity(pattern, path) {
    if (!pattern || !pattern.length) {
      return 25;
    }
    const normalizedPattern = pattern.filter((segment) => segment !== "*" && !/^\d+$/.test(segment));
    const normalizedPath = path.filter((segment) => segment !== "*" && !/^\d+$/.test(segment));
    if (!normalizedPattern.length || !normalizedPath.length) {
      return 25;
    }
    let expectedIndex = normalizedPattern.length - 1;
    let actualIndex = normalizedPath.length - 1;
    let matched = 0;
    while (expectedIndex >= 0 && actualIndex >= 0) {
      if (normalizedPattern[expectedIndex] === normalizedPath[actualIndex]) {
        matched += 1;
        expectedIndex -= 1;
        actualIndex -= 1;
        continue;
      }
      actualIndex -= 1;
    }
    if (!matched) {
      return 25;
    }
    return 25 + matched * 10;
  }
  function bestTargetMatchScore(matchers, surface, path) {
    if (!matchers.length) {
      return 0;
    }
    let best = -1;
    for (const matcher of matchers) {
      if (!matcher.surfaces.includes(surface)) {
        continue;
      }
      if (!matcher.path || !matcher.path.length) {
        best = Math.max(best, 0);
        continue;
      }
      const specificity = pathPatternSpecificity(matcher.path, path);
      best = Math.max(best, specificity);
    }
    return best;
  }
  function parseTargetMatchers(target) {
    if (!target.trim()) {
      return [];
    }
    return target.split(/[|,+]/).map((part) => part.trim()).filter(Boolean).flatMap(expandBraceExpression).map(parseTargetMatcher).filter((matcher) => matcher !== null);
  }
  function parseTargetMatcher(raw) {
    const token = raw.trim();
    if (!token) {
      return null;
    }
    for (const prefix of TARGET_PREFIXES) {
      if (token === prefix || token.startsWith(`${prefix}.`) || token.startsWith(`${prefix}[`)) {
        return {
          surfaces: resolveTargetSurfaces(prefix),
          path: token === prefix ? null : parsePath(token.slice(prefix.length + (token[prefix.length] === "[" ? 0 : 1)))
        };
      }
    }
    return {
      surfaces: ALL_SURFACES,
      path: parsePath(token)
    };
  }
  function resolveTargetSurfaces(prefix) {
    switch (prefix) {
      case "request":
        return ["request.headers", "request.body", "request.params", "request.query"];
      case "response":
        return ["response.body"];
      case "trace":
      case "log.payload":
        return ["trace.args", "trace.returnValue", "trace.error"];
      case "db.document":
      case "payload":
        return [
          "db.before",
          "db.after",
          "db.query",
          "db.resultMeta",
          "request.body",
          "response.body",
          "trace.args",
          "trace.returnValue"
        ];
      case "request.headers":
      case "request.body":
      case "request.params":
      case "request.query":
      case "response.body":
      case "trace.args":
      case "trace.returnValue":
      case "trace.error":
      case "db.pk":
      case "db.before":
      case "db.after":
      case "db.query":
      case "db.resultMeta":
      case "db.error":
        return [prefix];
      default:
        return ALL_SURFACES;
    }
  }
  function pathPatternMatches(pattern, path) {
    return pathPatternSpecificity(pattern, path) >= 0;
  }
  function pathPatternSpecificity(pattern, path) {
    if (!pattern.length) {
      return 0;
    }
    if (pattern.length > path.length) {
      return -1;
    }
    let specificity = 0;
    for (let index = 0; index < pattern.length; index += 1) {
      const expected = pattern[index];
      const actual = path[index];
      if (expected === "*") {
        continue;
      }
      if (expected !== actual) {
        return -1;
      }
      specificity += 1;
    }
    return specificity * 100 + pattern.length;
  }
  function parsePath(value) {
    return value.trim().replace(/^\./, "").replace(/\[(\d+|\*)?\]/g, (_match, part) => `.${part || "*"}`).split(".").map((part) => part.trim()).filter(Boolean);
  }
  function expandBraceExpression(value) {
    const match = value.match(/\{([^{}]+)\}/);
    if (!match) {
      return [value];
    }
    const variants = match[1].split(",").map((part) => part.trim()).filter(Boolean);
    if (!variants.length) {
      return [value.replace(match[0], "")];
    }
    return variants.flatMap((variant) => expandBraceExpression(value.replace(match[0], variant)));
  }
  function tokenizeValue(value, context, surface, path) {
    if (typeof value === "string") {
      if (isAlreadyPrivacyToken(value)) {
        return value;
      }
      if (looksLikeEmail(value)) {
        return redactEmailDeterministic(value);
      }
    }
    const namespace = `${surface}:${path.join(".") || "$"}`;
    const token = stableToken(namespace, value, context);
    if (typeof value === "string") {
      return `${path[path.length - 1] || "tok"}_tok_${token}`;
    }
    if (typeof value === "number" || typeof value === "bigint") {
      return `num_tok_${token}`;
    }
    if (typeof value === "boolean") {
      return value;
    }
    if (Array.isArray(value)) {
      return value.map(() => "[tokenized]");
    }
    if (value && typeof value === "object") {
      const output = {};
      for (const key of Object.keys(value)) {
        output[key] = "[tokenized]";
      }
      return output;
    }
    return {
      type: typeof value,
      token: `tok_${token}`
    };
  }
  function isAlreadyPrivacyToken(value) {
    return /^(?:email_tok_|[A-Za-z0-9_.-]+_tok_|num_tok_|tok_)[a-f0-9]{8,}$/i.test(value.trim());
  }
  var SUMMARY_MAX_DEPTH = 8;
  var SUMMARY_MAX_KEYS = 40;
  var SUMMARY_MAX_ITEMS = 10;
  var EXACT_GLUE_WORDS = /* @__PURE__ */ new Set([
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "be",
    "can",
    "could",
    "did",
    "do",
    "does",
    "for",
    "from",
    "had",
    "has",
    "have",
    "if",
    "in",
    "into",
    "is",
    "it",
    "must",
    "never",
    "not",
    "of",
    "on",
    "or",
    "should",
    "the",
    "then",
    "to",
    "up",
    "via",
    "was",
    "were",
    "will",
    "with",
    "would"
  ]);
  var SUMMARY_UNSAFE_PATH_PATTERNS = [
    /(^|\.)(description|message|comment|note|statusnote|failurereason|reason|error)$/i,
    /\bemail\b/i,
    /\bname\b/i,
    /\bphone\b/i,
    /\baddress\b/i,
    /\b(dob|birthdate|dateofbirth)\b/i,
    /\btoken\b/i,
    /\bsecret\b/i,
    /\bpassword\b/i,
    /\bconnection\b/i,
    /\bdsn\b/i,
    /\buri\b/i,
    /\burl\b/i,
    /\bcard(last4|number|expiry|cvv)?\b/i
  ];
  var DROP_SAFETY_PATH_PATTERNS = [
    /(^|\.)(authorization|x[-_]?apikey|x[-_]?api[-_]?key|apikey|api[-_]?key|appsecret|secret|token|password|cookie|sessionid|credential|privatekey|refresh|refresh[-_]?token)$/i,
    /(^|\.)([a-z0-9_-]*(auth|authorization|bearer)|[a-z0-9_-]*secret|[a-z0-9_-]*token|[a-z0-9_-]*password|[a-z0-9_-]*cookie|[a-z0-9_-]*credential)$/i,
    /(^|\.)(providerconnection|rawgatewayconnection|connectionstring|connectionuri|connectionurl|dsn|uri)$/i,
    /(^|\.)([a-z0-9_-]*(connection|dsn))$/i,
    /(^|\.)(cardlast4|cardnumber|card[-_]?number|fullpan|pan|cvv|cvc|securitycode|expiry)$/i
  ];
  var TOKENIZE_SAFETY_PATH_PATTERNS = [
    /(^|\.)([a-z0-9_-]*email|email)$/i,
    /(^|\.)(customername|customer[-_]?name|operatorname|operator[-_]?name|username|user[-_]?name|fullname|full[-_]?name|firstname|first[-_]?name|lastname|last[-_]?name)$/i,
    /(^|\.)(incidentownername|incident[-_]?owner[-_]?name|ownername|owner[-_]?name|displayname|display[-_]?name|contactname|contact[-_]?name|reviewername|reviewer[-_]?name|requestername|requester[-_]?name|approvername|approver[-_]?name|assigneename|assignee[-_]?name|analystname|analyst[-_]?name|agentname|agent[-_]?name|payername|payer[-_]?name|payeename|payee[-_]?name)$/i,
    /(^|\.)(accountid|account[-_]?id|customerid|customer[-_]?id|userid|user[-_]?id|personid|person[-_]?id|profileid|profile[-_]?id|contactid|contact[-_]?id|externaluserid|external[-_]?user[-_]?id|externalcustomerid|external[-_]?customer[-_]?id)$/i,
    /(^|\.)(customer|user|account|person|profile|contact|owner|incidentowner|operator|reviewer|requester|approver|assignee|analyst|agent|payer|payee)\.(id|name|displayname|fullname|firstname|lastname)$/i
  ];
  var FREE_TEXT_FALLBACK_PATH_PATTERNS = [
    /(^|\.)(description|message|comment|note|statusnote|failurereason|reason|error)$/i
  ];
  var OPERATIONAL_SAFE_PATH_PATTERNS = [
    /(^|\.)(_?id|taskid|paymentid|orderid|safeorderid|invoiceid|shipmentid|drillid|caseid)$/i,
    /(^|\.)(reference|paymentreference|messagekey|cachekey|rediskey|key)$/i,
    /(^|\.)(eventid|requestid|rid|traceid|spanid|parentspanid|correlationid)$/i,
    /(^|\.)(timestamp|firsttimestamp|maxtimestamp|logappendtime|createdat|updatedat|processedat|occurredat|receivedat|sentat|startedat|endedat|completedat|queuedat|scheduledat|duedate)$/i,
    /(^|\.)(x-repro-request-rid|x-repro-trace-id|x-bug-request-start|x-request-id|x-correlation-id)$/i,
    /(^|\.)(status|taskstatus|rawgatewaystatus|classification|currency|method|priority|risklevel|severity|safeseverity|level|scenario)$/i,
    /(^|\.)(queue|safequeue|lane|safelane|region|saferegion|stage|workflowstage|x-workflow-stage|featureflag|safefeatureflag|reasoncode|shard)$/i,
    /(^|\.)(provider|component|environment|step)$/i
  ];
  var OPERATIONAL_ENUM_PATH_PATTERNS = [
    /(^|\.)([a-z0-9_-]*(status|state|priority|severity|level|classification|category|scenario|phase|stage|mode|method|currency|queue|lane|region|environment|component|provider|service|source|channel|direction|reasoncode|featureflag|shard))$/i
  ];
  var OPERATIONAL_COUNT_PATH_PATTERNS = [
    /(^|\.)([a-z0-9_-]*(count|total|attempt|attempts|retry|retries|limit|offset|partition|size|index|position))$/i
  ];
  var OPERATIONAL_TIMESTAMP_PATH_PATTERNS = [
    /(^|\.)(timestamp|firsttimestamp|maxtimestamp|logappendtime|[a-z0-9_-]*(created|updated|processed|occurred|received|sent|started|ended|completed|queued|scheduled|due)at|duedate)$/i
  ];
  var OPERATIONAL_ID_VALUE_PATTERNS = [
    /^[0-9a-f]{24}$/i,
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    /^S_[0-9a-f-]{16,}$/i,
    /^A_\d+_[a-z0-9]+$/i,
    /^\d{10,14}-[a-z0-9]+(?:-[a-z0-9]+)?$/i,
    /^[A-Z][A-Z0-9]{1,15}-[A-Z0-9][A-Z0-9-]{1,31}$/,
    /^[a-z][a-z0-9_-]*(?::[a-z0-9_-]+){1,}:[A-Za-z0-9_.-]{4,}$/i
  ];
  var SERIALIZED_STRUCTURED_TEXT_MAX_LENGTH = 2e5;
  function summarizeValue(policy2, value, context, surface, path, statement) {
    const depth = path.length;
    if (typeof value === "string") {
      const textPolicy = statement == null ? void 0 : statement.textPolicy;
      if ((textPolicy == null ? void 0 : textPolicy.mode) === "KNOWN_TEMPLATE") {
        return applyKnownTextPolicy(textPolicy, value, context, surface, path);
      }
      let text = value;
      if (!(statement == null ? void 0 : statement.rawTextMode) && shouldKeepOperationalStringExact(value, path)) {
        return value;
      }
      if ((statement == null ? void 0 : statement.rawTextMode) === "REGEX_ASSISTED_EXACT" && policy2.rawTextHints.length) {
        return sanitizeRawTextExactSync(policy2, value, context, surface, path, statement.rawTextHintNames);
      }
      const detected = sanitizeTextWithKnownDetectorsSync(value, path);
      if (detected !== value) {
        text = detected;
      }
      if ((statement == null ? void 0 : statement.rawTextMode) === "SAFE_PARTIAL_MASK") {
        if (shouldKeepSafePartialMaskStringExact(text, path)) {
          return text;
        }
        return partiallyMaskUnknownString(text);
      }
      return text;
    }
    if (value === null || value === void 0) {
      return value;
    }
    if (typeof value === "number" || typeof value === "bigint") {
      return value;
    }
    if (typeof value === "boolean") {
      return value;
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (Array.isArray(value)) {
      const visibleItems = value.slice(0, SUMMARY_MAX_ITEMS).map(
        (item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), "SUMMARIZE")
      );
      if (value.length > SUMMARY_MAX_ITEMS) {
        visibleItems.push(`[truncated ${value.length - SUMMARY_MAX_ITEMS} items]`);
      }
      return visibleItems;
    }
    if (value instanceof Map) {
      const fields = {};
      let index = 0;
      for (const [key, item] of value.entries()) {
        if (index >= SUMMARY_MAX_KEYS) break;
        const normalizedKey = normalizeCollectionKeyForPrivacy(key, index);
        fields[normalizedKey] = transformValue(policy2, surface, item, context, path.concat(normalizedKey), "SUMMARIZE");
        index += 1;
      }
      if (value.size > SUMMARY_MAX_KEYS) {
        fields.__reproTruncatedKeys = value.size - SUMMARY_MAX_KEYS;
      }
      return fields;
    }
    if (value instanceof Set) {
      const visibleItems = Array.from(value.values()).slice(0, SUMMARY_MAX_ITEMS).map(
        (item, index) => transformValue(policy2, surface, item, context, path.concat(String(index)), "SUMMARIZE")
      );
      if (value.size > SUMMARY_MAX_ITEMS) {
        visibleItems.push(`[truncated ${value.size - SUMMARY_MAX_ITEMS} items]`);
      }
      return visibleItems;
    }
    if (isWeakCollection(value)) {
      return `[${getPrivacyConstructorName(value)}: uninspectable]`;
    }
    if (value instanceof Error) {
      const output = {
        name: value.name,
        code: typeof value.code === "string" || typeof value.code === "number" ? String(value.code) : void 0,
        status: typeof value.status === "number" ? value.status : typeof value.statusCode === "number" ? value.statusCode : void 0,
        message: typeof value.message === "string" ? transformValue(policy2, surface, value.message, context, path.concat("message"), "SUMMARIZE") : void 0
      };
      return output;
    }
    if (value && typeof value === "object") {
      if (depth >= SUMMARY_MAX_DEPTH) {
        return "[truncated object depth]";
      }
      const keys = Object.keys(value);
      const visibleKeys = keys.slice(0, SUMMARY_MAX_KEYS);
      const fields = {};
      for (const key of visibleKeys) {
        fields[key] = transformValue(policy2, surface, value[key], context, path.concat(key), "SUMMARIZE");
      }
      if (keys.length > SUMMARY_MAX_KEYS) {
        fields.__reproTruncatedKeys = keys.length - SUMMARY_MAX_KEYS;
      }
      return fields;
    }
    return value;
  }
  function applyKnownTextPolicy(policy2, value, context, surface, path) {
    if (policy2.mode !== "KNOWN_TEMPLATE" || !policy2.fragments.length) {
      return sanitizeTextWithKnownDetectorsSync(value, path);
    }
    let cursor = 0;
    let output = "";
    for (let index = 0; index < policy2.fragments.length; index += 1) {
      const fragment = policy2.fragments[index];
      if (fragment.kind === "literal") {
        if (!value.startsWith(fragment.text, cursor)) {
          return sanitizeTextWithKnownDetectorsSync(value, path);
        }
        output += fragment.text;
        cursor += fragment.text.length;
        continue;
      }
      const nextLiteral = findNextLiteral(policy2.fragments, index + 1);
      const end = nextLiteral ? value.indexOf(nextLiteral.text, cursor) : value.length;
      if (end < 0) {
        return sanitizeTextWithKnownDetectorsSync(value, path);
      }
      const raw = value.slice(cursor, end);
      output += applyTextSlotAction(fragment, raw, context, surface, path, index);
      cursor = end;
    }
    return cursor === value.length ? normalizePrivacyReplacementSpacing(output) : sanitizeTextWithKnownDetectorsSync(value, path);
  }
  function applyTextSlotAction(fragment, raw, context, surface, path, index) {
    var _a;
    switch (fragment.action) {
      case "KEEP_EXACT":
        return keepExactExceptKnownSensitiveText(raw);
      case "TOKENIZE":
        return String(tokenizeValue(raw, context, surface, path.concat((_a = fragment.label) != null ? _a : `slot${index}`)));
      case "DROP":
        return "[dropped]";
      case "PARTIAL_MASK":
      default:
        return partiallyMaskUnknownString(raw);
    }
  }
  function keepExactExceptKnownSensitiveText(value) {
    const spans = detectSensitiveTextSpans(value);
    if (!spans.length) {
      return value;
    }
    let cursor = 0;
    let output = "";
    for (const span of spans) {
      if (span.start > cursor) {
        output += value.slice(cursor, span.start);
      }
      output += span.replacement;
      cursor = span.end;
    }
    if (cursor < value.length) {
      output += value.slice(cursor);
    }
    return normalizePrivacyReplacementSpacing(output);
  }
  function sanitizeRawTextExactSync(policy2, value, context, surface, path, rawTextHintNames) {
    const hinted = applyRawTextHints(value, selectRawTextHints(policy2.rawTextHints, rawTextHintNames), context, surface, path);
    return sanitizeTextWithKnownDetectorsSync(hinted, path);
  }
  function selectRawTextHints(hints, rawTextHintNames) {
    if (!(rawTextHintNames == null ? void 0 : rawTextHintNames.length)) {
      return hints;
    }
    const allowed = new Set(rawTextHintNames.map((name) => name.trim()).filter(Boolean));
    const selected = hints.filter((hint) => allowed.has(hint.name));
    return selected.length ? selected : hints;
  }
  function applyRawTextHints(value, hints, context, surface, path) {
    if (!value || !hints.length) {
      return normalizePrivacyReplacementSpacing(value);
    }
    const spans = [];
    for (const hint of sortRawTextHintsForPath(hints, path)) {
      collectRawTextHintSpans(value, hint, context, surface, path, spans);
    }
    if (!spans.length) {
      return normalizePrivacyReplacementSpacing(value);
    }
    let cursor = 0;
    let output = "";
    for (const span of mergeSpans(spans)) {
      if (span.start > cursor) {
        output += value.slice(cursor, span.start);
      }
      output += span.replacement;
      cursor = span.end;
    }
    if (cursor < value.length) {
      output += value.slice(cursor);
    }
    return normalizePrivacyReplacementSpacing(output);
  }
  function sortRawTextHintsForPath(hints, path) {
    const normalizedPath = normalizeRawTextHintNeedle(path.join("."));
    return hints.slice().sort(
      (left, right) => rawTextHintPathScore(left, normalizedPath) - rawTextHintPathScore(right, normalizedPath)
    );
  }
  function rawTextHintPathScore(hint, normalizedPath) {
    const tokenLabel = normalizeRawTextHintNeedle(hint.tokenLabel);
    const name = normalizeRawTextHintNeedle(hint.name);
    if (tokenLabel && normalizedPath.includes(tokenLabel)) return 0;
    if (name && normalizedPath.includes(name)) return 1;
    return 2;
  }
  function normalizeRawTextHintNeedle(value) {
    return value.toLowerCase().replace(/[^a-z0-9]+/g, "");
  }
  function collectRawTextHintSpans(value, hint, context, surface, path, out) {
    const pattern = new RegExp(hint.pattern.source, hint.pattern.flags.includes("g") ? hint.pattern.flags : `${hint.pattern.flags}g`);
    let match;
    let guard = 0;
    while (match = pattern.exec(value)) {
      const matched = match[0];
      if (!matched) {
        pattern.lastIndex += 1;
        if (pattern.lastIndex > value.length || guard++ > 1e3) {
          break;
        }
        continue;
      }
      out.push({
        start: match.index,
        end: match.index + matched.length,
        replacement: hint.action === "DROP" ? "[dropped]" : String(tokenizeValue(matched, context, surface, path.concat(hint.tokenLabel)))
      });
      guard += 1;
      if (guard > 1e3) {
        break;
      }
    }
  }
  function findNextLiteral(fragments, startIndex) {
    for (let index = startIndex; index < fragments.length; index += 1) {
      const fragment = fragments[index];
      if (fragment.kind === "literal") {
        return fragment;
      }
    }
    return null;
  }
  function partiallyMaskUnknownString(value) {
    if (!value) {
      return value;
    }
    const spans = detectSensitiveTextSpans(value);
    if (!spans.length) {
      return maskGeneralTextSegment(value);
    }
    let cursor = 0;
    let output = "";
    for (const span of spans) {
      if (span.start > cursor) {
        output += maskGeneralTextSegment(value.slice(cursor, span.start));
      }
      output += span.replacement;
      cursor = span.end;
    }
    if (cursor < value.length) {
      output += maskGeneralTextSegment(value.slice(cursor));
    }
    return normalizePrivacyReplacementSpacing(output);
  }
  function normalizePrivacyReplacementSpacing(value) {
    if (!value || !value.includes("[") && !/_tok_/i.test(value)) {
      return value;
    }
    const bracketMarker = String.raw`(?:\[(?:dropped|secret-token|connection-string|auth-token|session-secret|private-key|basic-auth|jwt|credit-card|phone|person|ssn|masked)\])`;
    const tokenMarker = String.raw`(?:email_tok_[a-f0-9]{8,}|[A-Za-z][A-Za-z0-9_.-]*_tok_[a-f0-9]{8,})`;
    const glueWord = String.raw`(?:for|from|by|to|with|owner|user|customer|operator|account|email|name)`;
    const suffixGlueWord = String.raw`(?:and|or|on|for|from|to|with|can|was|were|is|in|by)`;
    return value.replace(new RegExp(`([A-Za-z0-9])(${bracketMarker})`, "g"), "$1 $2").replace(new RegExp(`(${bracketMarker})([A-Za-z0-9])`, "g"), "$1 $2").replace(new RegExp(`\\b(${glueWord})(${tokenMarker})`, "gi"), "$1 $2").replace(new RegExp(`(${tokenMarker})(${suffixGlueWord})\\b`, "gi"), "$1 $2");
  }
  function shouldKeepOperationalStringExact(value, path = []) {
    const trimmed = value.trim();
    if (!trimmed) {
      return true;
    }
    const comparablePath = normalizeComparablePath(path);
    if (comparablePath && SUMMARY_UNSAFE_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return false;
    }
    if (comparablePath && DROP_SAFETY_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return false;
    }
    if (comparablePath && OPERATIONAL_SAFE_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return true;
    }
    if (comparablePath && OPERATIONAL_TIMESTAMP_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return looksLikeOperationalTimestamp(trimmed);
    }
    if (comparablePath && OPERATIONAL_COUNT_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return isOperationalNumericScalar(trimmed);
    }
    if (comparablePath && OPERATIONAL_ENUM_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return isLowRiskOperationalScalar(trimmed);
    }
    if (containsObviousSensitiveText(trimmed)) {
      return false;
    }
    return OPERATIONAL_ID_VALUE_PATTERNS.some((pattern) => pattern.test(trimmed));
  }
  function containsObviousSensitiveText(value) {
    if (looksLikeEmail(value) || looksLikeConnectionString(value) || containsJwtLikeText(value) || containsBearerAuthText(value) || containsBasicAuthText(value) || containsPrivateKeyBlock(value)) {
      return true;
    }
    if (/@/.test(value) || /:\/\/\S+/.test(value)) {
      return true;
    }
    if (containsCreditCardLikeText(value)) {
      return true;
    }
    return false;
  }
  function shouldKeepSafePartialMaskStringExact(value, path) {
    const trimmed = value.trim();
    if (!trimmed) {
      return true;
    }
    if (shouldKeepOperationalStringExact(value, path)) {
      return true;
    }
    const comparablePath = normalizeComparablePath(path);
    if (!comparablePath) {
      return false;
    }
    if (SUMMARY_UNSAFE_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath)) || DROP_SAFETY_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath)) || TOKENIZE_SAFETY_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath)) || containsObviousSensitiveText(trimmed)) {
      return false;
    }
    if (OPERATIONAL_TIMESTAMP_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return looksLikeOperationalTimestamp(trimmed) || isLowRiskOperationalScalar(trimmed);
    }
    if (OPERATIONAL_COUNT_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return isOperationalNumericScalar(trimmed);
    }
    if (OPERATIONAL_ENUM_PATH_PATTERNS.some((pattern) => pattern.test(comparablePath))) {
      return isLowRiskOperationalScalar(trimmed);
    }
    return false;
  }
  function isLowRiskOperationalScalar(value) {
    if (!value || value.length > 96) {
      return false;
    }
    if (/\s/.test(value)) {
      return false;
    }
    if (containsObviousSensitiveText(value)) {
      return false;
    }
    return /^[A-Za-z0-9_.:/-]+$/.test(value);
  }
  function isOperationalNumericScalar(value) {
    return /^[+-]?\d+(?:\.\d+)?$/.test(value.trim());
  }
  function looksLikeOperationalTimestamp(value) {
    const trimmed = value.trim();
    return /^\d{4}-\d{2}-\d{2}(?:[T ][0-2]\d:[0-5]\d(?::[0-5]\d(?:\.\d{1,9})?)?(?:Z|[+-][0-2]\d:?[0-5]\d)?)?$/.test(trimmed) || /^\d{10,14}$/.test(trimmed);
  }
  function sanitizeTextWithKnownDetectorsSync(value, path = []) {
    if (shouldKeepOperationalStringExact(value, path)) {
      return value;
    }
    const structured = sanitizeSerializedStructuredText(value, path);
    if (structured !== null) {
      return structured;
    }
    let output = value;
    output = redactEmailsInText(output);
    output = replaceCreditCardCandidates(output, () => "[credit-card]");
    output = replaceJwtLikeText(output);
    output = replaceBearerAuthText(output);
    output = replaceBasicAuthText(output);
    output = replacePrivateKeyBlocks(output);
    output = output.replace(/\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis):\/\/[^\s"']+/gi, "[connection-string]");
    return normalizePrivacyReplacementSpacing(output);
  }
  function sanitizeSerializedStructuredText(value, path = []) {
    const parsed = parseSerializedStructuredText(value);
    if (parsed === null) {
      return null;
    }
    try {
      const sanitized = sanitizeStructuredValueWithKnownDetectors(parsed, path);
      if (!sanitized.changed) {
        return null;
      }
      return JSON.stringify(sanitized.value);
    } catch (e) {
      return null;
    }
  }
  function parseSerializedStructuredText(value) {
    if (value.length > SERIALIZED_STRUCTURED_TEXT_MAX_LENGTH) {
      return null;
    }
    const trimmed = value.trim();
    if (!trimmed || trimmed[0] !== "{" && trimmed[0] !== "[") {
      return null;
    }
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed === null || typeof parsed !== "object") {
        return null;
      }
      return parsed;
    } catch (e) {
      return null;
    }
  }
  function sanitizeStructuredValueWithKnownDetectors(value, path) {
    const objectId = coercePrivacyObjectId(value);
    if (objectId !== null) {
      return { value: objectId, changed: true };
    }
    if (Array.isArray(value)) {
      let changed = false;
      const next = value.map((item, index) => {
        const sanitized = sanitizeStructuredValueWithKnownDetectors(item, path.concat(String(index)));
        changed = changed || sanitized.changed;
        return sanitized.value;
      });
      return { value: next, changed };
    }
    if (value && typeof value === "object") {
      let changed = false;
      const next = {};
      for (const [key, child] of Object.entries(value)) {
        const sanitized = sanitizeStructuredValueWithKnownDetectors(child, path.concat(key));
        changed = changed || sanitized.changed;
        next[key] = sanitized.value;
      }
      return { value: next, changed };
    }
    const floorAction = deriveSensitiveLeafFloorAction(value, path);
    if (floorAction === "DROP") {
      return { value: "[dropped]", changed: true };
    }
    if (floorAction === "TOKENIZE") {
      if (typeof value === "string" && isAlreadyPrivacyToken(value)) {
        return { value, changed: false };
      }
      if (typeof value === "string" && looksLikeEmail(value)) {
        return { value: redactEmailDeterministic(value), changed: true };
      }
      const leaf = path[path.length - 1] || "value";
      const token = createHash("sha256").update(`repro|${normalizeComparablePath(path)}|${stableStringify(value)}`).digest("hex").slice(0, 12);
      return { value: `${leaf}_tok_${token}`, changed: true };
    }
    if (typeof value === "string") {
      const sanitized = sanitizeTextWithKnownDetectorsSync(value, path);
      if (sanitized !== value) {
        return { value: sanitized, changed: true };
      }
    }
    return { value, changed: false };
  }
  function maskGeneralTextSegment(value) {
    const placeholderPattern = /\[[a-z-]+\]/gi;
    let cursor = 0;
    let output = "";
    let match;
    while (match = placeholderPattern.exec(value)) {
      if (match.index > cursor) {
        output += maskPlainTextTokens(value.slice(cursor, match.index));
      }
      output += match[0];
      cursor = match.index + match[0].length;
    }
    if (cursor < value.length) {
      output += maskPlainTextTokens(value.slice(cursor));
    }
    return output;
  }
  function maskPlainTextTokens(value) {
    return value.replace(/[A-Za-z0-9]+(?:[._:/-][A-Za-z0-9]+)*/g, (token) => partiallyMaskToken(token));
  }
  function partiallyMaskToken(token) {
    const length = token.length;
    if (isAlreadyPrivacyToken(token)) {
      return token;
    }
    if (shouldKeepOperationalStringExact(token)) {
      return token;
    }
    if (EXACT_GLUE_WORDS.has(token.toLowerCase())) {
      return token;
    }
    if (length <= 3) {
      return "...";
    }
    if (/^[A-Za-z]+$/.test(token)) {
      return maskTokenMiddle(token, alphabeticMaskBounds(length));
    }
    return maskTokenMiddle(token, mixedMaskBounds(length));
  }
  function alphabeticMaskBounds(length) {
    if (length <= 4) {
      return { prefixLength: 1, suffixLength: 1 };
    }
    if (length <= 6) {
      return { prefixLength: 2, suffixLength: 2 };
    }
    if (length <= 8) {
      return { prefixLength: 3, suffixLength: 2 };
    }
    return { prefixLength: 3, suffixLength: 3 };
  }
  function mixedMaskBounds(length) {
    if (length <= 5) {
      return { prefixLength: 1, suffixLength: 1 };
    }
    if (length <= 8) {
      return { prefixLength: 2, suffixLength: 2 };
    }
    return { prefixLength: 3, suffixLength: 3 };
  }
  function maskTokenMiddle(token, bounds) {
    const prefixLength = Math.min(bounds.prefixLength, token.length);
    const suffixLength = Math.min(bounds.suffixLength, Math.max(0, token.length - prefixLength - 1));
    if (prefixLength + suffixLength >= token.length) {
      return `${token.slice(0, 1)}..${token.slice(-1)}`;
    }
    return `${token.slice(0, prefixLength)}..${token.slice(token.length - suffixLength)}`;
  }
  function detectSensitiveTextSpans(value) {
    const spans = [];
    collectPatternSpans(
      value,
      /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi,
      (match) => maskEmail(match),
      spans
    );
    collectCreditCardSpans(value, spans);
    return mergeSpans(spans);
  }
  function collectPatternSpans(value, pattern, replacer, out) {
    let match;
    const globalPattern = new RegExp(pattern.source, pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`);
    while (match = globalPattern.exec(value)) {
      const matched = match[0];
      if (!matched) {
        continue;
      }
      out.push({
        start: match.index,
        end: match.index + matched.length,
        replacement: replacer(matched)
      });
    }
  }
  function collectCreditCardSpans(value, out) {
    const pattern = /\b(?:\d[ -]*?){13,19}\b/g;
    let match;
    while (match = pattern.exec(value)) {
      const matched = match[0];
      if (!matched || shouldPreserveNumericCandidateInText(value, match.index, matched) || !isLuhnValidCardCandidate(matched)) {
        continue;
      }
      out.push({
        start: match.index,
        end: match.index + matched.length,
        replacement: maskCardLikeValue(matched)
      });
    }
  }
  function containsCreditCardLikeText(value) {
    const pattern = /\b(?:\d[ -]*?){13,19}\b/g;
    let match;
    while (match = pattern.exec(value)) {
      if (match[0] && isLuhnValidCardCandidate(match[0])) {
        return true;
      }
    }
    return false;
  }
  function replaceCreditCardCandidates(value, replacer) {
    return value.replace(/\b(?:\d[ -]*?){13,19}\b/g, (match, offset, fullText) => {
      if (shouldPreserveNumericCandidateInText(String(fullText), Number(offset), match)) {
        return match;
      }
      if (!isLuhnValidCardCandidate(match)) {
        return match;
      }
      return replacer(match);
    });
  }
  function shouldPreserveNumericCandidateInText(value, start, match) {
    const token = expandTokenAround(value, start, start + match.length);
    if (!token) {
      return false;
    }
    if (/^\d{10,14}-[a-z0-9]+(?:-[a-z0-9]+)?$/i.test(token)) {
      return true;
    }
    if (/^(?:req|rid|trace|span|request|event)[-_]?\d{10,}/i.test(token)) {
      return true;
    }
    if (/^\d{10,14}$/.test(token) && !isLuhnValidCardCandidate(token)) {
      return true;
    }
    return shouldKeepOperationalStringExact(token);
  }
  function isLuhnValidCardCandidate(value) {
    const digits = value.replace(/\D/g, "");
    if (digits.length < 13 || digits.length > 19) {
      return false;
    }
    let sum = 0;
    let doubleDigit = false;
    for (let index = digits.length - 1; index >= 0; index -= 1) {
      let digit = Number(digits[index]);
      if (!Number.isInteger(digit)) {
        return false;
      }
      if (doubleDigit) {
        digit *= 2;
        if (digit > 9) {
          digit -= 9;
        }
      }
      sum += digit;
      doubleDigit = !doubleDigit;
    }
    return sum % 10 === 0;
  }
  function expandTokenAround(value, start, end) {
    let left = start;
    while (left > 0 && /[A-Za-z0-9_-]/.test(value[left - 1])) {
      left -= 1;
    }
    let right = end;
    while (right < value.length && /[A-Za-z0-9_-]/.test(value[right])) {
      right += 1;
    }
    return value.slice(left, right);
  }
  function mergeSpans(spans) {
    const sorted = spans.slice().sort((left, right) => left.start - right.start || left.end - right.end);
    const merged = [];
    for (const span of sorted) {
      const last = merged[merged.length - 1];
      if (!last || span.start >= last.end) {
        merged.push(span);
        continue;
      }
      if (span.end > last.end) {
        last.end = span.end;
        last.replacement = span.replacement;
      }
    }
    return merged;
  }
  function maskEmail(value) {
    return redactEmailDeterministic(value);
  }
  function maskCardLikeValue(value) {
    const digits = value.replace(/\D/g, "");
    if (!digits.length) {
      return "[masked]";
    }
    if (digits.length <= 4) {
      return `${digits.slice(0, 1)}...${digits.slice(-1)}`;
    }
    return `${digits.slice(0, 2)}...${digits.slice(-2)}`;
  }
  function stableToken(namespace, value, context) {
    const secret = context.appSecret || context.appId || "repro";
    const material = `${secret}|${namespace}|${stableStringify(value)}`;
    return createHash("sha256").update(material).digest("hex").slice(0, 12);
  }
  function stableStringify(value) {
    if (value === void 0) return "undefined";
    if (value === null) return "null";
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (typeof value === "bigint") {
      return `${value.toString()}n`;
    }
    if (Array.isArray(value)) {
      return `[${value.map((item) => stableStringify(item)).join(",")}]`;
    }
    if (value && typeof value === "object") {
      return `{${Object.keys(value).sort().map((key) => `${key}:${stableStringify(value[key])}`).join(",")}}`;
    }
    return String(value);
  }
  function coercePrivacyObjectId(value) {
    var _a;
    if (value === null || value === void 0) {
      return null;
    }
    if (typeof value === "string") {
      const direct = normalizePrivacyObjectIdString(value);
      return direct && /^[0-9a-f]{24}$/i.test(direct) ? direct.toLowerCase() : null;
    }
    if (typeof value !== "object") {
      return null;
    }
    if (Array.isArray(value)) return null;
    const bsonType = typeof (value == null ? void 0 : value._bsontype) === "string" ? String(value._bsontype).toLowerCase() : "";
    const ctorName = typeof ((_a = value == null ? void 0 : value.constructor) == null ? void 0 : _a.name) === "string" ? String(value.constructor.name).toLowerCase() : "";
    const looksLikeObjectId = bsonType.includes("objectid") || ctorName === "objectid" || typeof value.toHexString === "function";
    const fromBufferShape = looksLikeObjectId ? coercePrivacyObjectIdFromBufferShape(value, true) : null;
    if (fromBufferShape) {
      return fromBufferShape;
    }
    if (typeof value.$oid === "string") {
      const oid = normalizePrivacyObjectIdString(value.$oid);
      if (oid && /^[0-9a-f]{24}$/i.test(oid)) {
        return oid.toLowerCase();
      }
    }
    try {
      if (typeof value.toHexString === "function") {
        const hex = normalizePrivacyObjectIdString(value.toHexString());
        if (hex && /^[0-9a-f]{24}$/i.test(hex)) {
          return hex.toLowerCase();
        }
      }
    } catch (e) {
    }
    try {
      if (typeof value.toString === "function") {
        const text = normalizePrivacyObjectIdString(value.toString());
        if (text && /^[0-9a-f]{24}$/i.test(text)) {
          return text.toLowerCase();
        }
      }
    } catch (e) {
    }
    return looksLikeObjectId ? "[mongo-id]" : null;
  }
  function normalizePrivacyObjectIdString(value) {
    if (value === null || value === void 0) {
      return null;
    }
    const text = String(value).trim();
    if (!text || text === "[object Object]") {
      return null;
    }
    return text;
  }
  function coercePrivacyObjectIdFromBufferShape(value, includeIdCandidate = false) {
    const decode = (source) => {
      if (!source) {
        return null;
      }
      if (typeof source === "string") {
        const normalized = normalizePrivacyObjectIdString(source);
        return normalized && /^[0-9a-f]{24}$/i.test(normalized) ? normalized.toLowerCase() : null;
      }
      if (typeof Buffer !== "undefined" && Buffer.isBuffer(source) || source instanceof Uint8Array) {
        if (source.length !== 12) {
          return null;
        }
        try {
          return Buffer.from(source).toString("hex");
        } catch (e) {
          return null;
        }
      }
      const bytes = Array.isArray(source) ? source : typeof source === "object" && (source == null ? void 0 : source.type) === "Buffer" && Array.isArray(source == null ? void 0 : source.data) ? source.data : typeof source === "object" ? Object.keys(source).filter((key) => /^\d+$/.test(key)).sort((left, right) => Number(left) - Number(right)).map((key) => source[key]) : null;
      if (!Array.isArray(bytes) || bytes.length !== 12) {
        return null;
      }
      if (bytes.some((byte) => typeof byte !== "number" || !Number.isInteger(byte) || byte < 0 || byte > 255)) {
        return null;
      }
      try {
        return Buffer.from(bytes).toString("hex");
      } catch (e) {
        return null;
      }
    };
    const candidates = includeIdCandidate ? [value, value.buffer, value.id, value.$oid] : [value, value.buffer, value.$oid];
    for (const candidate of candidates) {
      const decoded = decode(candidate);
      if (decoded) {
        return decoded;
      }
    }
    return null;
  }
  function looksLikeEmail(value) {
    return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(value.trim());
  }
  function looksLikeConnectionString(value) {
    return /^(?:mongodb(?:\+srv)?|postgres(?:ql)?|mysql|redis|amqp):\/\/\S+$/i.test(value);
  }
  function containsJwtLikeText(value) {
    return /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/.test(value);
  }
  function replaceJwtLikeText(value) {
    return value.replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, "[jwt]");
  }
  function containsBearerAuthText(value) {
    return /\bBearer\s+(?!\[(?:dropped|auth-token)\])[A-Za-z0-9._\-+/=]{8,}/i.test(value);
  }
  function replaceBearerAuthText(value) {
    return value.replace(/\bBearer\s+(?!\[(?:dropped|auth-token)\])[A-Za-z0-9._\-+/=]{8,}/gi, "[auth-token]");
  }
  function containsBasicAuthText(value) {
    return /\bBasic\s+(?!\[(?:dropped|basic-auth)\])[A-Za-z0-9+/=]{8,}/i.test(value);
  }
  function replaceBasicAuthText(value) {
    return value.replace(/\bBasic\s+(?!\[(?:dropped|basic-auth)\])[A-Za-z0-9+/=]{8,}/gi, "[basic-auth]");
  }
  function containsPrivateKeyBlock(value) {
    return /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]+?-----END [A-Z ]*PRIVATE KEY-----/.test(value);
  }
  function replacePrivateKeyBlocks(value) {
    return value.replace(/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]+?-----END [A-Z ]*PRIVATE KEY-----/g, "[private-key]");
  }
  function normalizeTextPolicy(input) {
    if (!input || typeof input !== "object") {
      return void 0;
    }
    const policy2 = input;
    if (policy2.mode !== "KNOWN_TEMPLATE" || !Array.isArray(policy2.fragments) || !policy2.fragments.length) {
      return void 0;
    }
    const fragments = policy2.fragments.map((fragment) => normalizeTextFragment(fragment)).filter((fragment) => fragment !== null);
    if (!fragments.length) {
      return void 0;
    }
    return {
      mode: "KNOWN_TEMPLATE",
      fragments
    };
  }
  function normalizeTextFragment(input) {
    if (!input || typeof input !== "object") {
      return null;
    }
    const fragment = input;
    if (fragment.kind === "literal") {
      return typeof fragment.text === "string" && fragment.text.length ? { kind: "literal", text: fragment.text } : null;
    }
    if (fragment.kind === "slot") {
      if (fragment.action !== "KEEP_EXACT" && fragment.action !== "TOKENIZE" && fragment.action !== "PARTIAL_MASK" && fragment.action !== "DROP") {
        return null;
      }
      return {
        kind: "slot",
        label: typeof fragment.label === "string" && fragment.label.trim() ? fragment.label.trim() : void 0,
        action: fragment.action
      };
    }
    return null;
  }
  function normalizeRawTextMode(input) {
    return input === "SAFE_PARTIAL_MASK" || input === "REGEX_ASSISTED_EXACT" ? input : void 0;
  }
  function normalizeRawTextHints(input) {
    if (!Array.isArray(input)) {
      return [];
    }
    const out = [];
    for (const entry of input) {
      const hint = normalizeRawTextHint(entry);
      if (hint) {
        out.push(hint);
      }
    }
    return out.slice(0, 50);
  }
  function normalizeRawTextHintNames(input) {
    if (!Array.isArray(input)) {
      return void 0;
    }
    const names = input.map((value) => typeof value === "string" ? value.trim() : "").filter(Boolean);
    return names.length ? Array.from(new Set(names)).slice(0, 50) : void 0;
  }
  function normalizeRawTextHint(input) {
    if (!input || typeof input !== "object") {
      return null;
    }
    const record = input;
    if (record.scope !== "RAW_TEXT_ONLY") {
      return null;
    }
    const action = record.action;
    if (action !== "TOKENIZE" && action !== "DROP") {
      return null;
    }
    const patternText = typeof record.regex === "string" ? record.regex : typeof record.pattern === "string" ? record.pattern : "";
    if (!isSafeRawTextRegexPattern(patternText)) {
      return null;
    }
    const pattern = safeRegExp(patternText);
    if (!pattern) {
      return null;
    }
    const name = typeof record.name === "string" && record.name.trim() ? record.name.trim() : "rawTextHint";
    const tokenLabel = normalizeTokenLabel(
      typeof record.tokenLabel === "string" && record.tokenLabel.trim() ? record.tokenLabel : name
    );
    return {
      name,
      action,
      pattern,
      tokenLabel
    };
  }
  function isSafeRawTextRegexPattern(pattern) {
    const trimmed = pattern.trim();
    if (!trimmed || trimmed.length > 500) {
      return false;
    }
    if (trimmed === ".*" || trimmed === ".+" || trimmed === "[\\s\\S]*" || trimmed === "[\\s\\S]+") {
      return false;
    }
    if (/\(\?<?[=!]/.test(trimmed)) {
      return false;
    }
    if (/\.\s*[+*{]/.test(trimmed)) {
      return false;
    }
    if (/\[[^\]]+\]\s*[+*]/.test(trimmed)) {
      return false;
    }
    if (/\[[^\]]+\]\s*\{\d+,\}/.test(trimmed)) {
      return false;
    }
    if (/\([^)]*[+*][^)]*\)\s*[+*{]/.test(trimmed)) {
      return false;
    }
    return true;
  }
  function normalizeTokenLabel(value) {
    const normalized = value.trim().replace(/[^A-Za-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
    return normalized || "rawTextHint";
  }
  function safeRegExp(pattern) {
    try {
      const inlineFlags = pattern.match(/^\(\?([imsu]+)\)/i);
      if (inlineFlags) {
        const normalizedFlags = Array.from(new Set(inlineFlags[1].toLowerCase().split(""))).join("");
        return new RegExp(pattern.slice(inlineFlags[0].length), normalizedFlags);
      }
      return new RegExp(pattern);
    } catch (e) {
      return void 0;
    }
  }

  // go-sdk/tools/privacy-entry.ts
  var policy = null;
  function configure(json) {
    var _a;
    policy = normalizeRuntimePrivacyPolicy(JSON.parse(json));
    return JSON.stringify({ fingerprint: policy == null ? void 0 : policy.fingerprint, statementCount: (_a = policy == null ? void 0 : policy.statements.length) != null ? _a : 0 });
  }
  function apply(surface, value, context) {
    return JSON.stringify(applyRuntimePrivacyPolicy(policy, surface, JSON.parse(value), JSON.parse(context)));
  }
  return __toCommonJS(privacy_entry_exports);
})();
