import dts from "bun-plugin-dts";

await Bun.$`rm -rf ../dist/*`.catch(() => {});

const { success, logs } = await Bun.build({
  target: "bun",
  outdir: "../dist",
  entrypoints: ["./index.ts"],
  sourcemap: "external",
  plugins: [dts()],
  minify: {
    whitespace: false,
    syntax: true,
    identifiers: false,
  },
  external: ["flatbuffers"],
});

if (logs.length) for (const log of logs) console.log(log);
if (!success) process.exit(1);

const { default: pkg } = await import("./package.json");
await Bun.write(
  "../dist/package.json",
  JSON.stringify({
    ...pkg,
    module: "index.js",
    devDependencies: {},
  })
);
await Bun.$`cp README.md ../dist/README.md`