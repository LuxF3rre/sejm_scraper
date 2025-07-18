{
  pkgs,
  inputs,
  ...
}:
let
  pkgs-unstable = import inputs.nixpkgs-unstable { system = pkgs.stdenv.system; };
in
{
  packages = [
    pkgs-unstable.python312Packages.python-lsp-server
    pkgs-unstable.gitleaks
    pkgs-unstable.ruff
    pkgs-unstable.ty
  ];

  languages.python = {
    enable = true;
    version = "3.12";
    uv = {
      enable = true;
      package = pkgs-unstable.uv;
      sync = {
        enable = true;
        allExtras = true;
      };
    };
  };

  dotenv.enable = true;

  git-hooks.hooks = {
    ruff.enable = true;
    ruff-format.enable = true;
    nil.enable = true;
    nixfmt-rfc-style.enable = true;
    markdownlint.enable = true;
    name-tests-test.enable = true;
    check-yaml.enable = true;
    check-toml.enable = true;
    check-json.enable = true;
    end-of-file-fixer.enable = true;
    trim-trailing-whitespace.enable = true;
    gitleaks = {
      enable = true;
      name = "Detect hardcoded secrets";
      entry = "gitleaks git --pre-commit --redact --staged --verbose";
      language = "system";
      pass_filenames = false;
    };
    ty = {
      enable = true;
      name = "ty static analysis";
      entry = "ty check";
      language = "system";
    };
  };

  enterShell = ''
    uv venv
    source .devenv/state/venv/bin/activate
    uv sync
  '';
}
