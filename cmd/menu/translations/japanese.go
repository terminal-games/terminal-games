// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package translations

import (
	"fmt"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
)

type Japanese struct{}

func (Japanese) HeaderTitle() string { return "Terminal Games" }
func (Japanese) TabGames() string    { return "ゲーム" }
func (Japanese) TabProfile() string  { return "プロフィール" }
func (Japanese) TabAbout() string    { return "概要" }
func (Japanese) WindowTooSmall() string {
	return "ウィンドウをもっと大きくしてください"
}
func (Japanese) UnknownTab() string { return "不明なタブです" }

func (Japanese) HelpQuit() string        { return "終了" }
func (Japanese) HelpNextTab() string     { return "次のタブ" }
func (Japanese) HelpPrevTab() string     { return "前のタブ" }
func (Japanese) HelpUp() string          { return "上へ" }
func (Japanese) HelpDown() string        { return "下へ" }
func (Japanese) HelpEdit() string        { return "編集" }
func (Japanese) HelpDelete() string      { return "削除" }
func (Japanese) HelpMoveUp() string      { return "上に移動" }
func (Japanese) HelpMoveDown() string    { return "下に移動" }
func (Japanese) HelpSave() string        { return "保存" }
func (Japanese) HelpCancel() string      { return "キャンセル" }
func (Japanese) HelpDone() string        { return "完了" }
func (Japanese) HelpNavigate() string    { return "移動" }
func (Japanese) HelpSelect() string      { return "選択" }
func (Japanese) HelpBack() string        { return "戻る" }
func (Japanese) HelpConfirm() string     { return "確認" }
func (Japanese) HelpFilter() string      { return "絞り込み" }
func (Japanese) HelpClearFilter() string { return "絞り込みを解除" }
func (Japanese) HelpApplyFilter() string { return "絞り込みを適用" }
func (Japanese) HelpPlay() string        { return "プレイ" }
func (Japanese) HelpPageUp() string      { return "1ページ上へ" }
func (Japanese) HelpPageDown() string    { return "1ページ下へ" }
func (Japanese) HelpDetailsUp() string   { return "詳細を上へスクロール" }
func (Japanese) HelpDetailsDown() string { return "詳細を下へスクロール" }

func (Japanese) GamesListTitle() string { return "すべてのゲーム" }
func (Japanese) GamesNoMatch() string {
	return "現在の絞り込み条件に一致するゲームはありません。"
}
func (Japanese) GamesByAuthor(author string) string {
	return "作者: " + author
}

func (Japanese) GamesActiveSessions(count int, sessionsKnown bool) string {
	prefix := ""
	if !sessionsKnown && count > 0 {
		prefix = "~"
	}
	return fmt.Sprintf("%s%d 件のアクティブなセッション", prefix, count)
}

func (t Japanese) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          t.HelpUp(),
		Down:        t.HelpDown(),
		Filter:      t.HelpFilter(),
		ClearFilter: t.HelpClearFilter(),
		ApplyFilter: t.HelpApplyFilter(),
		Cancel:      t.HelpCancel(),
		Games:       t.TabGames(),
		Of:          "中",
		FilterValue: t.HelpFilter(),
	}
}

func (Japanese) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: "スクリーンショット",
		EscToClose:  "ESC で閉じる",
	}
}

func (Japanese) PlayButton() string  { return "プレイ" }
func (Japanese) PlayLoading() string { return "読み込み中" }

func (Japanese) AboutTitle() string {
	return "どこからでも、ターミナルでゲームをプレイ。"
}

func (Japanese) AboutBody() string {
	return "Terminal Games は、現代的なターミナルや SSH 上で動作するよう設計されたゲームのためのオープンソースプラットフォームです。"
}

func (Japanese) AboutDeveloperDocs(link string) string {
	return link + " でゲーム開発者になろう"
}

func (Japanese) AboutCreditsHeading() string { return "クレジット" }

func (Japanese) AboutDevelopedBy(name string) string {
	return "開発者: " + name
}

func (Japanese) AboutOpenSource(link string) string {
	return "オープンソース: " + link
}

func (Japanese) AboutServerVersionLabel() string   { return "サーバーバージョン" }
func (Japanese) AboutMenuVersionLabel() string     { return "メニューバージョン" }
func (Japanese) AboutCLIAPIVersionLabel() string   { return "CLI API バージョン" }
func (Japanese) AboutConnectedRegionLabel() string { return "接続中のリージョン" }
func (Japanese) AboutLocalOnly() string            { return "ローカルのみ" }
func (Japanese) AboutRegionHeader() string         { return "リージョン" }
func (Japanese) AboutLatencyHeader() string        { return "レイテンシ" }
func (Japanese) AboutSessionsHeader() string       { return "セッション" }
func (Japanese) AboutNodesHeader() string          { return "ノード" }
func (Japanese) AboutNetworkTopologyTitle() string { return "ネットワークトポロジ" }

func (Japanese) ProfileLoading() string { return "読み込み中..." }
func (Japanese) ProfileLoadFailed() string {
	return "プロフィールの読み込みに失敗しました。"
}
func (Japanese) ProfileUsername() string        { return "ユーザー名" }
func (Japanese) ProfileLanguages() string       { return "言語" }
func (Japanese) ProfileEnterToEditHint() string { return "  Enter で編集" }
func (Japanese) ProfileLanguageSearchPlaceholder() string {
	return "検索..."
}
func (Japanese) ProfileAddLanguage() string    { return "+ 言語を追加" }
func (Japanese) ProfileEnterToAddHint() string { return "  Enter で追加" }
func (Japanese) ProfileNoLanguageMatches() string {
	return "一致する項目はありません"
}

func (Japanese) ProfileReplaysTitle(count int) string {
	if count <= 0 {
		return "リプレイ"
	}
	return fmt.Sprintf("リプレイ (%d)", count)
}

func (Japanese) ProfileNoReplays() string       { return "  まだリプレイはありません。" }
func (Japanese) ProfileDeleteConfirm() string   { return "削除しますか？ y/n" }
func (Japanese) ProfileUnknownGame() string     { return "不明" }
func (Japanese) ProfileNamePlaceholder() string { return "ユーザー名を入力" }

func (Japanese) AboutNetworkSummary(regionCount, sessionCount int) string {
	return fmt.Sprintf("%d リージョン、%d 件のアクティブなセッション", regionCount, sessionCount)
}
