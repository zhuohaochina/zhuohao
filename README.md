# zhuohao

巨潮/东方财富数据采集与接口字段管理工具。

## 新电脑初始化

先从 GitHub 拉取代码：

```powershell
git clone https://github.com/zhuohaochina/zhuohao.git
cd zhuohao
```

安装根目录、后端、前端依赖：

```powershell
npm install
cd server
npm install
cd ../client
npm install
cd ..
```

导入接口与页面配置：

```powershell
npm run config:import
```

这个命令会从 `server/config/app-config.json` 恢复：

- 首页接口管理配置
- 字段中文名
- 列排序
- 隐藏列
- 字段格式化、单位、值映射、长文本折叠
- 组合视图配置

采集出来的数据不会随 GitHub 同步。导入配置后，启动项目并在页面里点击更新数据即可重新采集。

## 启动项目

可以使用项目里的 `manage.bat` 启动，也可以分别启动后端和前端。

后端：

```powershell
cd server
npm run dev
```

前端：

```powershell
cd client
npm run dev
```

## 同步配置到 GitHub

如果在页面里修改了接口配置、字段中文名、列顺序、隐藏列或组合视图，需要先导出配置：

```powershell
npm run config:export
```

然后提交并推送：

```powershell
git add server/config/app-config.json
git commit -m "Update app config"
git push
```

另一台电脑同步配置：

```powershell
git pull
npm run config:import
```

## 不上传的内容

以下内容只保留在本地，不上传 GitHub：

- `server/data/*.db`
- `server/data/*.db-wal`
- `server/data/*.db-shm`
- `server/.env`
- `node_modules/`
- `dist/`

