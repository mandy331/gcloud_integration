## 新手上路

### Markdown

文件用Markdown語法寫在這裡。

### 第一次的git

假設你已經照著說明建立好venv了，可以按照以下幾個步驟操作：

1. 起始化 git 資料夾
```console
git init

# 會出現以下訊息
Initialized empty Git repository in /app/admanager_report/.git/
```
2. 設定遠端檔案路徑
```console

git remote add origin https://github.com/Shihyen/admanager_report.git

```
3. 更新到最新的版本
```console
git pull origin master

# 會出現以下訊息
remote: Enumerating objects: 9, done.
remote: Counting objects: 100% (9/9), done.
remote: Compressing objects: 100% (7/7), done.
remote: Total 9 (delta 0), reused 0 (delta 0), pack-reused 0
Unpacking objects: 100% (9/9), done.
From https://github.com/Shihyen/admanager_report
 * branch            master     -> FETCH_HEAD
 * [new branch]      master     -> origin/master

```

4. 切換到自己的分支(範例：feature/auth)
```console
git checkout -b feature/auth

# 會出現以下訊息
Switched to a new branch 'feature/auth'

```


5. 改完程式之後把資料更新上去
```console
git add .

# 確認一下剛才新增的內容
git status

# 會出現以下訊息
On branch feature/auth
Changes to be committed:
  (use "git reset HEAD <file>..." to unstage)

	new file:   xxxxxx (剛才新增的檔案)

```

6. 打上一個版本
```console
git commit -m '這是今天的工作內容，只是把東西更新上來而已'

# 會出現以下訊息
[feature/auth 1b10510] 這是今天的工作內容，只是把東西更新上來而已
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 test
 
```


7. 更新到遠端，終於用到push了
```console
git push origin feature/auth

# 會出現以下訊息
Enumerating objects: 4, done.
Counting objects: 100% (4/4), done.
Delta compression using up to 4 threads
Compressing objects: 100% (2/2), done.
Writing objects: 100% (3/3), 344 bytes | 344.00 KiB/s, done.
Total 3 (delta 1), reused 0 (delta 0)
remote: Resolving deltas: 100% (1/1), completed with 1 local object.
remote:
remote: Create a pull request for 'feature/auth' on GitHub by visiting:
remote:      https://github.com/Shihyen/admanager_report/pull/new/feature/auth
remote:
To https://github.com/Shihyen/admanager_report.git
 * [new branch]      feature/auth -> feature/auth
 
```

8. 然後你要回去github的頁面

大概是這個網址：https://github.com/Shihyen/admanager_report/compare?expand=1

Open a pull request 底下的compare要選你剛剛push的branch，base選master，表示你要對master發出PR

稍微確認一下訊息，沒問題的話就可以按下最下方的Create pull request

