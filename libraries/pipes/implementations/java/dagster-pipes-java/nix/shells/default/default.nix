{
  lib,
  stdenv,
  pkgs,
  mkShell,
  ...
}:
let python = pkgs.python310; in
mkShell {
  # Create your shell
  packages = with pkgs; [
    zsh
    jdk8
    uv
    python
  ];
  shellHook = ''
    export PYTHONPATH=`pwd`/$VENV/${python.sitePackages}/:$PYTHONPATH
    export LD_LIBRARY_PATH=${lib.makeLibraryPath [stdenv.cc.cc]}
    export UV_PYTHON=${python}/bin/python
  '';
}
