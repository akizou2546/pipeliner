# python ./pipeline/make_exe.pyで実行
if __name__ == "__main__":
    import PyInstaller.__main__

    PyInstaller.__main__.run(
        ["./pipeline/__main__.py", "--onefile", "--noconfirm", "--clean"]
    )
