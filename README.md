# pipeline
A data pipeline.

# 使い方
1. gitでリポジトリを作成する
    ```
    git init --bare --shared /path/to/hogehoge.git
    ```
1. gitでリポジトリをクローンする
    ```
    git clone file:///path/to/remote_repo local_repo
    ```
1. condaで新規に環境を作る
    ```
    conda create --yes --prefix .conda python
    ```
1. pipで関連パッケージをインストールする
    ```
    cd ./
    pip install -e .[exe]
    ```
1. pyinstallerでexeファイルを作成する
    ```
    python ./pipeline/main_pyinstaller.py
    ```
1. 