declare const reproSHA256: (value: string) => string;
export function createHash(algorithm: string) {
  if (algorithm !== 'sha256') throw new Error('Unsupported digest');
  let value = '';
  return {
    update(input: string) { value += input; return this; },
    digest(encoding: string) {
      if (encoding !== 'hex') throw new Error('Unsupported encoding');
      return reproSHA256(value);
    },
  };
}
