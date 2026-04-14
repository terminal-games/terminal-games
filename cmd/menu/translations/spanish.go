// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package translations

import (
	"fmt"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
)

type Spanish struct{}

func (Spanish) HeaderTitle() string    { return "Terminal Games" }
func (Spanish) TabGames() string       { return "Juegos" }
func (Spanish) TabProfile() string     { return "Perfil" }
func (Spanish) TabAbout() string       { return "Acerca de" }
func (Spanish) WindowTooSmall() string { return "La ventana debe ser más grande" }
func (Spanish) UnknownTab() string     { return "Pestaña desconocida" }

func (Spanish) HelpQuit() string        { return "salir" }
func (Spanish) HelpNextTab() string     { return "siguiente pestaña" }
func (Spanish) HelpPrevTab() string     { return "pestaña anterior" }
func (Spanish) HelpAudio() string       { return "ayuda de audio" }
func (Spanish) HelpDismiss() string     { return "ocultar ayuda de audio" }
func (Spanish) HelpUp() string          { return "arriba" }
func (Spanish) HelpDown() string        { return "abajo" }
func (Spanish) HelpEdit() string        { return "editar" }
func (Spanish) HelpDelete() string      { return "eliminar" }
func (Spanish) HelpMoveUp() string      { return "mover arriba" }
func (Spanish) HelpMoveDown() string    { return "mover abajo" }
func (Spanish) HelpSave() string        { return "guardar" }
func (Spanish) HelpCancel() string      { return "cancelar" }
func (Spanish) HelpDone() string        { return "listo" }
func (Spanish) HelpNavigate() string    { return "navegar" }
func (Spanish) HelpSelect() string      { return "seleccionar" }
func (Spanish) HelpBack() string        { return "volver" }
func (Spanish) HelpConfirm() string     { return "confirmar" }
func (Spanish) HelpFilter() string      { return "filtrar" }
func (Spanish) HelpClearFilter() string { return "limpiar filtro" }
func (Spanish) HelpApplyFilter() string { return "aplicar filtro" }
func (Spanish) HelpPlay() string        { return "jugar" }
func (Spanish) HelpPageUp() string      { return "subir una página" }
func (Spanish) HelpPageDown() string    { return "bajar una página" }
func (Spanish) HelpDetailsUp() string   { return "subir detalles" }
func (Spanish) HelpDetailsDown() string { return "bajar detalles" }

func (Spanish) GamesListTitle() string { return "Todos los juegos" }
func (Spanish) GamesNoMatch() string {
	return "Ningún juego coincide con el filtro actual."
}
func (Spanish) GamesByAuthor(author string) string {
	return "por " + author
}

func (Spanish) GamesActiveSessions(count int, sessionsKnown bool) string {
	prefix := ""
	if !sessionsKnown && count > 0 {
		prefix = "~"
	}
	if count == 1 {
		return prefix + "1 sesión activa"
	}
	return fmt.Sprintf("%s%d sesiones activas", prefix, count)
}

func (t Spanish) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          t.HelpUp(),
		Down:        t.HelpDown(),
		Filter:      t.HelpFilter(),
		ClearFilter: t.HelpClearFilter(),
		ApplyFilter: t.HelpApplyFilter(),
		Cancel:      t.HelpCancel(),
		Games:       t.TabGames(),
		Of:          "de",
		FilterValue: t.HelpFilter(),
	}
}

func (Spanish) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: "Capturas de pantalla",
		EscToClose:  "ESC para cerrar",
	}
}

func (Spanish) PlayButton() string  { return "Jugar" }
func (Spanish) PlayLoading() string { return "Cargando" }

func (Spanish) AboutTitle() string {
	return "Juega desde cualquier lugar, en la terminal."
}

func (Spanish) AboutBody() string {
	return "Terminal Games es una plataforma de código abierto creada para juegos diseñados para funcionar en terminales modernas y a través de SSH."
}

func (Spanish) AboutDeveloperDocs(link string) string {
	return "Conviértete en desarrollador de juegos en " + link
}

func (Spanish) AboutCreditsHeading() string { return "Créditos" }

func (Spanish) AboutDevelopedBy(name string) string {
	return "Desarrollado por " + name
}

func (Spanish) AboutOpenSource(link string) string {
	return "Código abierto en " + link
}

func (Spanish) AboutServerVersionLabel() string   { return "Versión del servidor" }
func (Spanish) AboutMenuVersionLabel() string     { return "Versión del menú" }
func (Spanish) AboutCLIAPIVersionLabel() string   { return "Versión de la API de la CLI" }
func (Spanish) AboutConnectedRegionLabel() string { return "Región conectada" }
func (Spanish) AboutLocalOnly() string            { return "solo local" }
func (Spanish) AboutRegionHeader() string         { return "Región" }
func (Spanish) AboutLatencyHeader() string        { return "Latencia" }
func (Spanish) AboutSessionsHeader() string       { return "Sesiones" }
func (Spanish) AboutNodesHeader() string          { return "Nodos" }
func (Spanish) AboutNetworkTopologyTitle() string { return "Topología de red" }

func (Spanish) SSHAudioCallout() string     { return "Cómo activar el audio por SSH" }
func (Spanish) SSHAudioDialogTitle() string { return "Activar el audio por SSH" }
func (Spanish) SSHAudioDialogInstall() string {
	return "Si todavía no tienes ffplay instalado, instálalo primero en tu equipo."
}
func (Spanish) SSHAudioDialogBody() string {
	return "Sal de esta sesión y vuelve a conectarte con uno de estos comandos:"
}
func (Spanish) SSHAudioDialogHint() string { return "Pulsa Esc para cerrar" }
func (Spanish) SSHAudioDialogKeyboardHint() string {
	return "[b] copiar bash  [f] copiar fish  [esc] cerrar"
}
func (Spanish) SSHAudioDialogBashLabel() string { return "Bash / Zsh" }
func (Spanish) SSHAudioDialogFishLabel() string { return "Fish" }
func (Spanish) SSHAudioDialogCopy() string      { return "Copiar" }
func (Spanish) SSHAudioDialogCopied() string    { return "Copiado" }
func (Spanish) SSHAudioDialogCopyBashHelp() string {
	return "copiar bash"
}
func (Spanish) SSHAudioDialogCopyFishHelp() string {
	return "copiar fish"
}
func (Spanish) SSHAudioDialogClose() string     { return "Cerrar" }
func (Spanish) SSHAudioDialogCloseHelp() string { return "cerrar" }

func (Spanish) ProfileLoading() string         { return "Cargando..." }
func (Spanish) ProfileLoadFailed() string      { return "No se pudo cargar el perfil." }
func (Spanish) ProfileUsername() string        { return "Nombre de usuario" }
func (Spanish) ProfileLanguages() string       { return "Idiomas" }
func (Spanish) ProfileEnterToEditHint() string { return "  Enter para editar" }
func (Spanish) ProfileLanguageSearchPlaceholder() string {
	return "buscar..."
}
func (Spanish) ProfileAddLanguage() string    { return "+ añadir idioma" }
func (Spanish) ProfileEnterToAddHint() string { return "  Enter para añadir" }
func (Spanish) ProfileNoLanguageMatches() string {
	return "sin coincidencias"
}

func (Spanish) ProfileReplaysTitle(count int) string {
	if count <= 0 {
		return "Repeticiones"
	}
	return fmt.Sprintf("Repeticiones (%d)", count)
}

func (Spanish) ProfileNoReplays() string       { return "  Aún no hay repeticiones." }
func (Spanish) ProfileDeleteConfirm() string   { return "¿eliminar? y/n" }
func (Spanish) ProfileUnknownGame() string     { return "desconocido" }
func (Spanish) ProfileNamePlaceholder() string { return "introduce un nombre de usuario" }

func (Spanish) AboutNetworkSummary(regionCount, sessionCount int) string {
	sessionLabel := "sesiones activas"
	if sessionCount == 1 {
		sessionLabel = "sesión activa"
	}
	return fmt.Sprintf("%d regiones, %d %s", regionCount, sessionCount, sessionLabel)
}
