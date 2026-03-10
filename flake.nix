# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };

        commonShell = pkgs.mkShell {
          packages = with pkgs; [
            rust-toolchain
            go
            gopls
            go-tools
            wasm-tools
            go-task
            flyctl
            openssl
            opentofu
            tofu-ls
            ansible
            lego
            alejandra
          ];

          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
          ];

          buildInputs = with pkgs; [
            clang
            libopus
          ];

          GOOS = "wasip1";
          GOARCH = "wasm";
        };
      in rec {
        packages.terminal-games-server = pkgs.rustPlatform.buildRustPackage {
          pname = "terminal-games-server";
          version = "0.1.0";
          src = ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
          };
          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
          ];
          buildInputs = with pkgs; [
            libopus
          ];
          buildAndTestSubdir = "terminal-games-server";
        };

        apps.terminal-games-server = {
          type = "app";
          program = "${packages.terminal-games-server}/bin/terminal-games-server";
        };

        packages.terminal-games-cli = pkgs.rustPlatform.buildRustPackage {
          pname = "terminal-games-cli";
          version = "0.1.0";
          src = ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
          };
          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
          ];
          buildInputs = with pkgs; [
            libopus
          ];
          buildAndTestSubdir = "terminal-games-cli";
        };

        apps.terminal-games-cli = {
          type = "app";
          program = "${packages.terminal-games-cli}/bin/terminal-games-cli";
        };

        devShells.default =
          if pkgs.stdenv.isLinux
          then
            pkgs.mkShell {
              inputsFrom = [commonShell];

              packages = with pkgs; [
                mold
                pkgsStatic.stdenv.cc
              ];

              buildInputs = with pkgs; [
                pkgsStatic.libopus
              ];

              LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
              CMAKE_POLICY_VERSION_MINIMUM = "3.5";
              CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${pkgs.pkgsStatic.stdenv.cc}/bin/${pkgs.pkgsStatic.stdenv.cc.targetPrefix}cc";
              CC_x86_64_unknown_linux_musl = "${pkgs.pkgsStatic.stdenv.cc}/bin/${pkgs.pkgsStatic.stdenv.cc.targetPrefix}cc";
              CXX_x86_64_unknown_linux_musl = "${pkgs.pkgsStatic.stdenv.cc}/bin/${pkgs.pkgsStatic.stdenv.cc.targetPrefix}c++";
              AR_x86_64_unknown_linux_musl = "${pkgs.pkgsStatic.stdenv.cc}/bin/${pkgs.pkgsStatic.stdenv.cc.targetPrefix}ar";
              PKG_CONFIG_ALLOW_CROSS = "1";
            }
          else
            pkgs.mkShell {
              inputsFrom = [commonShell];
            };
      }
    );
}
