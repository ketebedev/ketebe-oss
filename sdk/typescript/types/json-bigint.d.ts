declare module "json-bigint" {
  interface Options { useNativeBigInt?: boolean; alwaysParseAsBig?: boolean }
  interface JsonBig { parse(text: string): unknown; stringify(value: unknown): string }
  export default function JSONBig(options?: Options): JsonBig;
}
