{
  pkgs,
  inputs,
  ...
}:
let
  pkgs-unstable = import inputs.nixpkgs-unstable { system = pkgs.stdenv.system; };
in
{
  dotenv.enable = true;
  claude.code.enable = true;

  env.UV_VENV_CLEAR = 1;

  packages = [
    pkgs-unstable.python312Packages.python-lsp-server
    pkgs-unstable.python312Packages.ipython
    pkgs-unstable.python312Packages.ipdb
    pkgs-unstable.python312Packages.scalene
    pkgs-unstable.ruff
    pkgs-unstable.ty
    pkgs-unstable.commitizen
    pkgs-unstable.gitleaks
    pkgs-unstable.cruft
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

  git-hooks.hooks = {
    ruff.enable = true;
    ruff-format.enable = true;
    ty = {
      enable = true;
      name = "ty static analysis";
      entry = "ty check";
      language = "system";
    };
    name-tests-test.enable = true;
    uv-lock = {
      enable = true;
      package = pkgs-unstable.uv;
    };
    uv-export = {
      enable = true;
      package = pkgs-unstable.uv;
      entry = "${pkgs-unstable.uv}/bin/uv export --format requirements.txt -o requirements.txt --locked --quiet";
    };
    uv-sync = {
      enable = true;
      package = pkgs-unstable.uv;
      entry = "${pkgs-unstable.uv}/bin/uv sync";
      stages = [
        "pre-commit"
        "post-checkout"
        "post-merge"
        "post-rewrite"
      ];
      pass_filenames = false;
    };
    nil.enable = true;
    nixfmt-rfc-style.enable = true;
    markdownlint.enable = true;
    check-yaml.enable = true;
    check-toml.enable = true;
    check-json.enable = true;
    end-of-file-fixer.enable = true;
    trim-trailing-whitespace.enable = true;
    fix-byte-order-marker.enable = true;
    mixed-line-endings.enable = true;
    commitizen = {
      enable = true;
      stages = [ "commit-msg" ];
    };
    gitleaks = {
      enable = true;
      name = "Detect hardcoded secrets";
      entry = "gitleaks git --pre-commit --redact --staged --verbose";
      language = "system";
      pass_filenames = false;
    };
  };

  enterShell = ''
    uv venv
    source .devenv/state/venv/bin/activate
    uv sync
    uv pip install -e .
  '';
}
