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

        rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in rec {
        devShells.default = pkgs.mkShell {
          name = "terminal-games";
          packages = with pkgs; [
            rust-toolchain
            go
            gopls
            go-tools
            wasm-tools
            go-task
            flyctl
          ];
          GOOS = "wasip1";
          GOARCH = "wasm";
        };
      }
    );
}
