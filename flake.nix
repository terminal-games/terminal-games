{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = {
    nixpkgs,
    utils,
    rust-overlay,
    ...
  }:
    utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [(import rust-overlay)];
        };

        wit-bindgen-go = pkgs.buildGoModule rec {
          pname = "wit-bindgen-go";
          version = "0.7.0";
          subPackages = [ "cmd/wit-bindgen-go" ];
          src = pkgs.fetchFromGitHub {
            owner = "bytecodealliance";
            repo = "go-modules";
            rev = "v${version}";
            sha256 = "sha256-bzsB0EsDNk6x1xroIQqbUy7L97JbEJHo7wASnl35X+0=";
          };
          vendorHash = "sha256-R4BdPlcaQBhH3cpLq//aeS3F2Qe4Z/TV/TALs6OSnAQ=";

          GOWORK = "off";
        };

        rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in rec {
        devShells.default = pkgs.mkShell {
          name = "terminal-games";
          packages = with pkgs; [
            rust-toolchain
            go
            gopls
            go-tools
            tinygo
            wasm-tools
            wit-bindgen-go
            wkg
            go-task
            flyctl
          ];
        };
      }
    );
}
