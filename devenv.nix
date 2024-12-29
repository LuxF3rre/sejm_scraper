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
   ];

  services.postgres = {
    enable = true;
    package = pkgs-unstable.postgresql;
    initialDatabases = [
      {
        name = "postgres";
        user = "postgres";
        pass = "postgres";
      }
    ];
    listen_addresses = "127.0.0.1";
    port = 5432;
  };
  
  languages.python = {
    enable = true;
    version = "3.12";
    poetry = {
      enable = true;
      package = pkgs-unstable.poetry;
      activate.enable = true;
      install = {
        enable = true;
        allExtras = true;
        installRootPackage = true;
      };
    };
  };
}
