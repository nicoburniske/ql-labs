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
              dbus.dev
              gst_all_1.gstreamer
              gst_all_1.gst-plugins-base
              gst_all_1.gst-plugins-good
              pipewire
              pkg-config
              toolchain
            ];
            GST_PLUGIN_SYSTEM_PATH_1_0 = pkgs.lib.makeSearchPath "lib/gstreamer-1.0" (with pkgs; [
              gst_all_1.gst-plugins-base
              gst_all_1.gst-plugins-good
              pipewire
            ]);
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath (with pkgs; [
              dbus
              glib
              gst_all_1.gstreamer
              gst_all_1.gst-plugins-base
              libxkbcommon
              libx11
              libxcursor
              libxi
              libxrandr
              wayland
            ]);
          };
        });
    };
}
