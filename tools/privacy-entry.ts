import { normalizeRuntimePrivacyPolicy, applyRuntimePrivacyPolicy } from '../../node-sdk/src/privacy';

let policy: ReturnType<typeof normalizeRuntimePrivacyPolicy> = null;
export function configure(json: string) {
  policy = normalizeRuntimePrivacyPolicy(JSON.parse(json));
  return JSON.stringify({ fingerprint: policy?.fingerprint, statementCount: policy?.statements.length ?? 0 });
}
export function apply(surface: string, value: string, context: string) {
  return JSON.stringify(applyRuntimePrivacyPolicy(policy, surface as any, JSON.parse(value), JSON.parse(context)));
}
