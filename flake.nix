{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, fenix, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
    in {
      devShells = nixpkgs.lib.genAttrs systems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          toolchain = fenix.packages.${system}.fromToolchainFile {
            file = self + "/rust-toolchain.toml";
            sha256 = "sha256-NvWKV8CXj8AQXESvz5uGr6qv0JF0UHUdjYb2murEG/A=";
          };
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              nushell
              toolchain
            ];
          };
        });
    };
}
