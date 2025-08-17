{
  pkgs,
  inputs,
  ...
}:

{
  dotenv.enable = true;
  claude.code.enable = true;
  devcontainer.enable = true;

  env = {
    SEJM_SCRAPER_DEBUG = "true";
    DUCKDB_PATH = "sejm_scraper.duckdb";
  };

  packages = [
    pkgs.python312Packages.python-lsp-server
    pkgs.python312Packages.ipython
    pkgs.python312Packages.ipdb
    pkgs.python312Packages.scalene
    pkgs.ruff
    pkgs.ty
    pkgs.commitizen
    pkgs.gitleaks
    pkgs.cruft
    pkgs.duckdb
  ];

  languages.python = {
    enable = true;
    version = "3.12";
    uv = {
      enable = true;
      package = pkgs.uv;
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
      entry = "bash -c '${pkgs.ty}/bin/ty check --python \${UV_PROJECT_ENVIRONMENT#$PWD/}'";
      language = "system";
    };
    uv-sync = {
      enable = true;
      entry = "${pkgs.uv}/bin/uv sync";
      stages = [
        "pre-commit"
        "post-checkout"
        "post-merge"
        "post-rewrite"
      ];
      pass_filenames = false;
    };
    uv-export = {
      enable = true;
      entry = "${pkgs.uv}/bin/uv export --format requirements.txt -o requirements.txt --quiet";
      after = [ "uv-sync" ];
    };
    # name-tests-test.enable = true;
    nil.enable = true;
    nixfmt-rfc-style.enable = true;
    markdownlint.enable = true;
    check-yaml.enable = true;
    check-toml.enable = true;
    check-json.enable = true;
    end-of-file-fixer.enable = true;
    trim-trailing-whitespace.enable = true;
    fix-byte-order-marker.enable = true;
    mixed-line-endings = {
      enable = true;
      args = [
        "--fix"
        "lf"
      ];
    };
    commitizen = {
      enable = true;
      stages = [ "commit-msg" ];
    };
    gitleaks = {
      enable = true;
      name = "Detect hardcoded secrets";
      entry = "${pkgs.gitleaks}/bin/gitleaks git --pre-commit --redact --staged --verbose";
      language = "system";
      pass_filenames = false;
    };
  };

  # if not using cachix you may need to install and expose
  # dynamically linked dependencies, like this in enterShell
  # export LD_LIBRARY_PATH=${pkgs.zlib}/lib:${pkgs.stdenv.cc.cc.lib}/lib:$LD_LIBRARY_PATH
  enterShell = ''
    source $UV_PROJECT_ENVIRONMENT/bin/activate
  '';

  enterTest = ''
    pytest -v --cov --cov-branch --cov-report=xml
  '';
}
