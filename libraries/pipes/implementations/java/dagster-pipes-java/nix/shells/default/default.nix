{
  pkgs,
  mkShell,
  ...
}:
mkShell {
  # Create your shell
  packages = with pkgs; [
    zsh
    jdk8
    uv
  ];
  shellHook = ''
    exec zsh
  '';
}
