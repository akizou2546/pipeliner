if __name__ == "__main__":
    import PyInstaller.__main__

    PyInstaller.__main__.run(
        ["./pipeline/__main__.py", "--onefile", "--noconfirm", "--clean"]
    )
