// @ts-check

import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  eslint.configs.recommended,
  tseslint.configs.recommended,
  {
    ignores: [
        "**/node_modules/",
        "**/.venv/",
        "**/dist/",
        "example-project/external_typescript_code/**/*.js",
        "*.config.js"
    ],
  },
  {
    rules: {
        "@typescript-eslint/no-explicit-any": "off"
    }
  }
);