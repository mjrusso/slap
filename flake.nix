{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        beam = pkgs.beamMinimal29Packages;

        basePackages = with pkgs; [
          git
          just
          curl
          nixpkgs-fmt

          # slap_yjs needs OTP 26.2 or later.
          beam.erlang
          beam.elixir_1_20

          rustc
          cargo
          rustfmt
          clippy

          nodejs_22
          pnpm

          leiningen
          jdk21_headless
          gnuplot
          graphviz
        ];

        devPackages = with pkgs; [
          beam.elixir-ls
          rust-analyzer
        ];

        env = {
          ERL_AFLAGS = "-kernel shell_history enabled";
          LANG = "C.UTF-8";
          SLAP_LOCAL_DEPS = "1";
          SLAP_SLATEDB_BUILD = "1";
        };

        devEnv = env // {
          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };

        # Mix and Hex keep their archives and caches in the repository, so
        # they match this shell's Elixir and OTP. `just deps` installs them.
        shellHook = ''
          export MIX_HOME="$PWD/.nix-mix"
          export HEX_HOME="$PWD/.nix-hex"
          export PATH="$MIX_HOME/bin:$MIX_HOME/escripts:$PATH"
        '';

      in
      {
        devShells = {

          # A stripped-down dev shell, for use in CI environments.
          #
          # Example usage:
          #
          #     nix develop .#ci -c COMMAND
          ci = pkgs.mkShell {
            packages = basePackages;
            inherit env shellHook;
          };

          default = pkgs.mkShell {
            packages = basePackages ++ devPackages
              ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux
              (with pkgs; [ inotify-tools libnotify ])
              ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [ pkgs.terminal-notifier ];

            env = devEnv;
            inherit shellHook;
          };

        };
      });
}
