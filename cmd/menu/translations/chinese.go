// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package translations

import (
	"fmt"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
)

type Chinese struct{}

func (Chinese) HeaderTitle() string    { return "Terminal Games" }
func (Chinese) TabGames() string       { return "游戏" }
func (Chinese) TabProfile() string     { return "个人资料" }
func (Chinese) TabAbout() string       { return "关于" }
func (Chinese) WindowTooSmall() string { return "窗口需要更大" }
func (Chinese) UnknownTab() string     { return "未知标签页" }

func (Chinese) HelpQuit() string        { return "退出" }
func (Chinese) HelpNextTab() string     { return "下一个标签页" }
func (Chinese) HelpPrevTab() string     { return "上一个标签页" }
func (Chinese) HelpUp() string          { return "上" }
func (Chinese) HelpDown() string        { return "下" }
func (Chinese) HelpEdit() string        { return "编辑" }
func (Chinese) HelpDelete() string      { return "删除" }
func (Chinese) HelpMoveUp() string      { return "上移" }
func (Chinese) HelpMoveDown() string    { return "下移" }
func (Chinese) HelpSave() string        { return "保存" }
func (Chinese) HelpCancel() string      { return "取消" }
func (Chinese) HelpDone() string        { return "完成" }
func (Chinese) HelpNavigate() string    { return "导航" }
func (Chinese) HelpSelect() string      { return "选择" }
func (Chinese) HelpBack() string        { return "返回" }
func (Chinese) HelpConfirm() string     { return "确认" }
func (Chinese) HelpFilter() string      { return "筛选" }
func (Chinese) HelpClearFilter() string { return "清除筛选" }
func (Chinese) HelpApplyFilter() string { return "应用筛选" }
func (Chinese) HelpPlay() string        { return "开始游戏" }
func (Chinese) HelpPageUp() string      { return "向上翻页" }
func (Chinese) HelpPageDown() string    { return "向下翻页" }
func (Chinese) HelpDetailsUp() string   { return "详情上滚" }
func (Chinese) HelpDetailsDown() string { return "详情下滚" }

func (Chinese) GamesListTitle() string { return "所有游戏" }
func (Chinese) GamesNoMatch() string   { return "没有游戏符合当前筛选条件。" }
func (Chinese) GamesByAuthor(author string) string {
	return "作者：" + author
}

func (Chinese) GamesActiveSessions(count int, sessionsKnown bool) string {
	prefix := ""
	if !sessionsKnown && count > 0 {
		prefix = "~"
	}
	return fmt.Sprintf("%s%d 个活跃会话", prefix, count)
}

func (t Chinese) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          t.HelpUp(),
		Down:        t.HelpDown(),
		Filter:      t.HelpFilter(),
		ClearFilter: t.HelpClearFilter(),
		ApplyFilter: t.HelpApplyFilter(),
		Cancel:      t.HelpCancel(),
		Games:       t.TabGames(),
		Of:          "共",
		FilterValue: t.HelpFilter(),
	}
}

func (Chinese) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: "截图",
		EscToClose:  "按 ESC 关闭",
	}
}

func (Chinese) PlayButton() string  { return "开始游戏" }
func (Chinese) PlayLoading() string { return "加载中" }

func (Chinese) AboutTitle() string {
	return "随时随地，在终端中玩游戏。"
}

func (Chinese) AboutBody() string {
	return "Terminal Games 是一个开源平台，专为可在现代终端和 SSH 环境中运行的游戏而打造。"
}

func (Chinese) AboutDeveloperDocs(link string) string {
	return "前往 " + link + " 成为游戏开发者"
}

func (Chinese) AboutCreditsHeading() string { return "致谢" }

func (Chinese) AboutDevelopedBy(name string) string {
	return "开发者：" + name
}

func (Chinese) AboutOpenSource(link string) string {
	return "开源地址：" + link
}

func (Chinese) AboutServerVersionLabel() string   { return "服务器版本" }
func (Chinese) AboutMenuVersionLabel() string     { return "菜单版本" }
func (Chinese) AboutCLIAPIVersionLabel() string   { return "CLI API 版本" }
func (Chinese) AboutConnectedRegionLabel() string { return "已连接区域" }
func (Chinese) AboutLocalOnly() string            { return "仅本地" }
func (Chinese) AboutRegionHeader() string         { return "区域" }
func (Chinese) AboutLatencyHeader() string        { return "延迟" }
func (Chinese) AboutSessionsHeader() string       { return "会话" }
func (Chinese) AboutNodesHeader() string          { return "节点" }
func (Chinese) AboutNetworkTopologyTitle() string { return "网络拓扑" }

func (Chinese) ProfileLoading() string         { return "加载中..." }
func (Chinese) ProfileLoadFailed() string      { return "加载个人资料失败。" }
func (Chinese) ProfileUsername() string        { return "用户名" }
func (Chinese) ProfileLanguages() string       { return "语言" }
func (Chinese) ProfileEnterToEditHint() string { return "  按 Enter 编辑" }
func (Chinese) ProfileLanguageSearchPlaceholder() string {
	return "搜索..."
}
func (Chinese) ProfileAddLanguage() string    { return "+ 添加语言" }
func (Chinese) ProfileEnterToAddHint() string { return "  按 Enter 添加" }
func (Chinese) ProfileNoLanguageMatches() string {
	return "无匹配项"
}

func (Chinese) ProfileReplaysTitle(count int) string {
	if count <= 0 {
		return "回放"
	}
	return fmt.Sprintf("回放（%d）", count)
}

func (Chinese) ProfileNoReplays() string       { return "  还没有回放。" }
func (Chinese) ProfileDeleteConfirm() string   { return "删除？y/n" }
func (Chinese) ProfileUnknownGame() string     { return "未知" }
func (Chinese) ProfileNamePlaceholder() string { return "输入用户名" }

func (Chinese) AboutNetworkSummary(regionCount, sessionCount int) string {
	return fmt.Sprintf("%d 个区域，%d 个活跃会话", regionCount, sessionCount)
}
