#### 一、配置示例

```yaml
x-default-logging: &default-logging
  driver: "json-file"
  options:
    max-size: "100m"
    max-file: "3"

x-default-environment: &default-environment
  TZ: Asia/Shanghai
  LANG: C.UTF-8

services:
  watchtower:
    image: containrrr/watchtower
    container_name: watchtower
    restart: always
    command: lucky homepage
    volumes:
        - /var/run/docker.sock:/var/run/docker.sock
    environment:
      <<: *default-environment
      WATCHTOWER_RUN_ONCE: false
      WATCHTOWER_SCHEDULE: 0 0 4 * * *
      WATCHTOWER_NOTIFICATION_REPORT: true
      # HOST 为 MoviePilot 服务地址，PORT 为 MoviePilot 服务端口（默认 3001），APIKEY 为 MoviePilot API Token
      WATCHTOWER_NOTIFICATION_URL: generic+http://HOST:PORT/api/v1/plugin/WatchtowerNotify/webhook?apikey=APIKEY
      # 通知模板配置，仅供参考
      WATCHTOWER_NOTIFICATION_TEMPLATE: |
        {{if .Report}}
          {{- with .Report}}
            {{- if ( or .Updated .Failed .Skipped )}}
        📊 更新统计：
          • 📦 扫描容器：{{len .Scanned}}
          • 🍃 无需更新：{{len .Fresh}}
          • ✅ 更新成功：{{len .Updated}}
          • ❌ 更新失败：{{len .Failed}}
          • ⚠️ 跳过更新：{{len .Skipped}}
        {{"\n"}}
        📝 更新详情：
          {{- range .Updated}}
          • ✅ {{.Name}} Updated
          {{- end}}
          {{- range .Failed}}
          • ❌ {{.Name}} Failed: {{.Error}}
          {{- end}}
          {{- range .Skipped}}
          • ⚠️ {{.Name}} Skipped: {{.Error}}
          {{- end}}
            {{end}}
          {{end}}
        {{ else }}
          {{- range .Entries}}
          • {{.Message}}
          {{- end }}
        {{ end }}
    logging: *default-logging
```

#### 二、示例效果

```
🚀 Watchtower 通知

📊 更新统计：
  • 📦 扫描容器：2
  • 🍃 无需更新：1
  • ✅ 更新成功：1
  • ❌ 更新失败：0
  • ⚠️ 跳过更新：0

📝 更新详情：
  • ✅ /homepage Updated
```