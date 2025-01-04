{
  lib,
  inputs,
  stdenv,
  pkgs,
  mkShell,
  ...
}:
let python = pkgs.python310; in
mkShell {
  packages = with pkgs; [
    zsh
    uv
    python
    pkg-config
    (
      rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
        extensions = [
          "rust-src"
          "rust-analyzer"
        ];
      })
    )
    cargo-nextest
  ];
  shellHook = ''
    export PYTHONPATH=`pwd`/$VENV/${python.sitePackages}/:$PYTHONPATH
    export LD_LIBRARY_PATH=${lib.makeLibraryPath [stdenv.cc.cc]}
    export UV_PYTHON=${python}/bin/python
  '';
}
