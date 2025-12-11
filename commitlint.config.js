module.exports = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "scope-enum": [
      2,
      "always",
      [
        // Crates
        "core",
        "consensus",
        "storage",
        "node",
        "proto",
        // Infrastructure
        "ci",
        "deps",
        // Documentation
        "docs",
      ],
    ],
    "scope-empty": [1, "never"], // Warn if no scope
    "body-max-line-length": [0], // Disable line length limit
  },
};
