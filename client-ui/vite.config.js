import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [vue()],
  base: "/client-ui/",
  build: {
    outDir: resolve(__dirname, "../static/client"),
    emptyOutDir: true,
  },
  server: {
    port: 5176,
    proxy: {
      "/client/": "http://127.0.0.1:5555",
    },
  },
});
