{
  description = "Dagster Pipes Java flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    snowfall-lib = {
      url = "github:snowfallorg/lib";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs:
    inputs.snowfall-lib.mkFlake {
      # You must provide our flake inputs to Snowfall Lib.
      inherit inputs;

      # The `src` must be the root of the flake. See configuration
      # in the next section for information on how you can move your
      # Nix files to a separate directory.
      src = ./.;

      formatter = {
        x86_64-linux = inputs.nixpkgs.legacyPackages.x86_64-linux.alejandra;
        aarch64-linux = inputs.nixpkgs.legacyPackages.aarch64-linux.alejandra;
        x86_64-darwin = inputs.nixpkgs.legacyPackages.x86_64-darwin.alejandra;
        aarch64-darwin = inputs.nixpkgs.legacyPackages.aarch64-darwin.alejandra;
      };

      # Configure Snowfall Lib, all of these settings are optional.
      snowfall = {
        # All Nix files are in the `nix` directory.
        root = ./nix;
        # Add flake metadata that can be processed by tools like Snowfall Frost.
        meta = {
          # A slug to use in documentation when displaying things like file paths.
          name = "dagster-pipes-java";

          # A title to show for your flake, typically the name.
          title = "Flake for Dagster Pipes Java";
        };

        outputs-builder = channels: {
          # Outputs in the outputs builder are transformed to support each system. This
          # entry will be turned into multiple different outputs like `formatter.x86_64-linux.*`.
          formatter = channels.nixpkgs.alejandra;
        };
      };
    };
}
