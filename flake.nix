{
  description = "A new Python project (uv-managed; Nix dev shell + uv2nix build).";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs = {
        pyproject-nix.follows = "pyproject-nix";
        nixpkgs.follows = "nixpkgs";
      };
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs = {
        pyproject-nix.follows = "pyproject-nix";
        uv2nix.follows = "uv2nix";
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
    }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = lib.genAttrs systems;
      pkgsFor = system: nixpkgs.legacyPackages.${system};

      # Python interpreter follows pyproject.toml's `requires-python`: the
      # first PEP 440 spec is taken as the floor (handles `>=X.Y` and
      # `>=X.Y,<Z.W` shapes). Bump pyproject.toml — flake follows.
      project = pyproject-nix.lib.project.loadPyproject { projectRoot = ./.; };
      lowerBound = lib.head project.requires-python;
      pyAttr =
        "python"
        + toString (builtins.elemAt lowerBound.version.release 0)
        + toString (builtins.elemAt lowerBound.version.release 1);

      # uv.lock → Nix. Wheels preferred (faster, matches what `uv sync` resolves).
      # Switch to "sdist" if a dep ships a broken wheel and needs local build.
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      overlay = workspace.mkPyprojectOverlay { sourcePreference = "wheel"; };

      # Editable variant: project source resolved from $REPO_ROOT at shell
      # entry, not baked into the store. Source edits show up immediately;
      # uv.lock changes still require re-entering the shell.
      editableOverlay = workspace.mkEditablePyprojectOverlay { root = "$REPO_ROOT"; };

      pythonSets = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
          python = pkgs.${pyAttr};
        in
        (pkgs.callPackage pyproject-nix.build.packages { inherit python; }).overrideScope (
          lib.composeManyExtensions [
            pyproject-build-systems.overlays.wheel
            overlay
          ]
        )
      );
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
          python = pkgs.${pyAttr};

          # Editable pythonSet — only used by `.#pure`. Hatchling's
          # `build_editable` hook imports the `editables` package; inject it
          # as a build input here (not in the base set — production builds
          # don't need it).
          editablePythonSet = pythonSets.${system}.overrideScope (
            lib.composeManyExtensions [
              editableOverlay
              (final: prev: {
                myproject = prev.myproject.overrideAttrs (old: {
                  nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ final.resolveBuildSystem { editables = [ ]; };
                });
              })
            ]
          );
          editableVenv = editablePythonSet.mkVirtualEnv "myproject-dev-env" workspace.deps.all;
        in
        {
          # Default shell: uv-managed. Matches the inner loop (`make fix`/`check`)
          # without rebuilding the Nix venv on every dep tweak. `uv add` / `uv lock`
          # work natively; `uv sync` populates `.venv` on first entry.
          default = pkgs.mkShell {
            packages = [
              python
              pkgs.uv
              pkgs.gnumake
            ];

            # uv's bundled python-build-standalone won't link on NixOS;
            # force the Nix-managed interpreter. No-op on Darwin.
            shellHook = ''
              export UV_PYTHON=${python}/bin/python3
              export UV_PYTHON_PREFERENCE=only-system
              echo "🐍 python: $(python3 --version)"
              echo "📦 uv:     $(uv --version)"
            '';
          };

          # Pure shell: uv2nix-built venv with the project installed editable.
          # No `.venv` needed. Source edits to src/myproject/ are live. Bumping
          # uv.lock requires exiting and re-entering so Nix re-resolves.
          # `uv lock --upgrade` works inside; `uv sync` is suppressed by UV_NO_SYNC.
          pure = pkgs.mkShell {
            packages = [
              editableVenv
              pkgs.uv
              pkgs.gnumake
            ];
            env = {
              UV_NO_SYNC = "1";
              UV_PYTHON = "${editableVenv}/bin/python";
              UV_PYTHON_DOWNLOADS = "never";
            };
            shellHook = ''
              unset PYTHONPATH
              export REPO_ROOT=$(git rev-parse --show-toplevel)
              echo "🐍 python: $(python --version) (nix-built, editable)"
              echo "📦 uv:     $(uv --version)"
            '';
          };
        }
      );

      # `nix build .#default` produces a runtime venv (project + deps in
      # `[project.dependencies]`). `.#dev` adds `[dependency-groups].dev`
      # (basedpyright, ruff, pytest, ...). Both are Nix-built — no uv at
      # build time — suitable for nixpkgs PRs / NixOS modules / consumers.
      packages = forAllSystems (system: {
        default = pythonSets.${system}.mkVirtualEnv "myproject-env" workspace.deps.default;
        dev = pythonSets.${system}.mkVirtualEnv "myproject-dev-env" workspace.deps.all;
      });

      # `nix fmt` autoformats flake.nix to RFC-166 style.
      # Paired with checks.${system}.nixfmt below, which verifies it stayed formatted.
      formatter = forAllSystems (system: (pkgsFor system).nixfmt);

      # `nix flake check` runs these. `runCommand … touch $out` is the
      # idiomatic pass/fail pattern: tool exits non-zero → derivation fails.
      checks = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          statix = pkgs.runCommand "check-statix" { nativeBuildInputs = [ pkgs.statix ]; } ''
            statix check ${./flake.nix}
            touch $out
          '';
          nixfmt = pkgs.runCommand "check-nixfmt" { nativeBuildInputs = [ pkgs.nixfmt ]; } ''
            nixfmt --check ${./flake.nix}
            touch $out
          '';
        }
      );
    };
}
