if __name__ == "__main__":
    import PyInstaller.__main__

    PyInstaller.__main__.run(
        [
            "./pipeliner/__main__.py",
            "--name=pipeliner",
            "--onefile",
            "--noconfirm",
            "--clean",
            "--hiddenimport=pyarrow",
            "--hiddenimport=xlsx2csv",
        ]
    )
