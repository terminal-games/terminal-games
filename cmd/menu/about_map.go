// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"math"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

const (
	brailleCellWidth   = 2
	brailleCellHeight  = 4
	brailleUnicodeBase = 0x2800
)

type geoPoint struct {
	Lon float64
	Lat float64
}

type mapStyle int

const (
	mapWater mapStyle = iota
	mapLand
	mapLine
	mapPeer
	mapCurrent
	mapLabel
)

type mapCell struct {
	r     rune
	style mapStyle
}

type worldCanvas struct {
	w     int
	h     int
	cells []mapCell
}

func newWorldCanvas(w, h int) worldCanvas {
	cells := make([]mapCell, w*h)
	for i := range cells {
		cells[i] = mapCell{r: ' ', style: mapWater}
	}
	return worldCanvas{w: w, h: h, cells: cells}
}

func (c *worldCanvas) inBounds(x, y int) bool {
	return x >= 0 && x < c.w && y >= 0 && y < c.h
}

func (c *worldCanvas) index(x, y int) int {
	return y*c.w + x
}

func (c *worldCanvas) set(x, y int, r rune, style mapStyle) {
	if !c.inBounds(x, y) {
		return
	}
	c.cells[c.index(x, y)] = mapCell{r: r, style: style}
}

func (c *worldCanvas) text(x, y int, text string, style mapStyle) {
	for i, r := range text {
		if c.inBounds(x+i, y) {
			c.set(x+i, y, r, style)
		}
	}
}

func (c *worldCanvas) render(styles map[mapStyle]lipgloss.Style) string {
	lines := make([]string, c.h)
	for y := range c.h {
		var b strings.Builder
		for x := range c.w {
			cell := c.cells[c.index(x, y)]
			rendered := string(cell.r)
			if style, ok := styles[cell.style]; ok && cell.r != ' ' {
				rendered = style.Render(rendered)
			}
			b.WriteString(rendered)
		}
		lines[y] = b.String()
	}
	return strings.Join(lines, "\n")
}

type brailleCell struct {
	mask     byte
	style    mapStyle
	priority int
}

type brailleRaster struct {
	w     int
	h     int
	cells []brailleCell
}

func newBrailleRaster(w, h int) brailleRaster {
	return brailleRaster{
		w:     max(1, w),
		h:     max(1, h),
		cells: make([]brailleCell, max(1, w*h)),
	}
}

func (r *brailleRaster) paint(dotX, dotY int, style mapStyle, priority int) {
	if dotX < 0 || dotY < 0 || dotX >= r.w*brailleCellWidth || dotY >= r.h*brailleCellHeight {
		return
	}
	cellX := dotX / brailleCellWidth
	cellY := dotY / brailleCellHeight
	index := cellY*r.w + cellX
	bit := brailleMask(dotX%brailleCellWidth, dotY%brailleCellHeight)
	cell := &r.cells[index]
	cell.mask |= bit
	if cell.mask == bit || priority >= cell.priority {
		cell.style = style
		cell.priority = priority
	}
}

func (r *brailleRaster) line(dotX0, dotY0, dotX1, dotY1 int, style mapStyle, priority int) {
	dx := absInt(dotX1 - dotX0)
	dy := absInt(dotY1 - dotY0)
	sx := 1
	if dotX0 > dotX1 {
		sx = -1
	}
	sy := 1
	if dotY0 > dotY1 {
		sy = -1
	}
	err := dx - dy

	for {
		r.paint(dotX0, dotY0, style, priority)
		if dotX0 == dotX1 && dotY0 == dotY1 {
			return
		}
		e2 := err * 2
		nextX := dotX0
		nextY := dotY0
		if e2 > -dy {
			err -= dy
			nextX += sx
		}
		if e2 < dx {
			err += dx
			nextY += sy
		}
		dotX0 = nextX
		dotY0 = nextY
	}
}

func (r *brailleRaster) stamp(dotX, dotY int, offsets []geoPoint, style mapStyle, priority int) {
	for _, offset := range offsets {
		r.paint(dotX+int(offset.Lon), dotY+int(offset.Lat), style, priority)
	}
}

func (r *brailleRaster) toCanvas() worldCanvas {
	canvas := newWorldCanvas(r.w, r.h)
	for y := 0; y < r.h; y++ {
		for x := 0; x < r.w; x++ {
			cell := r.cells[y*r.w+x]
			if cell.mask == 0 {
				continue
			}
			canvas.set(x, y, rune(brailleUnicodeBase)+rune(cell.mask), cell.style)
		}
	}
	return canvas
}

func overlayCanvas(dst *worldCanvas, src worldCanvas) {
	for y := 0; y < min(dst.h, src.h); y++ {
		for x := 0; x < min(dst.w, src.w); x++ {
			cell := src.cells[src.index(x, y)]
			if cell.r == ' ' {
				continue
			}
			dst.set(x, y, cell.r, cell.style)
		}
	}
}

func brailleMask(dotX, dotY int) byte {
	rowMajor := dotX + brailleCellWidth*dotY
	return [...]byte{0x01, 0x08, 0x02, 0x10, 0x04, 0x20, 0x40, 0x80}[rowMajor]
}

func mapProjectDots(width, height int, lon, lat float64) (int, int) {
	const minLon = -180.0
	const maxLon = 180.0
	const minLat = -90.0
	const maxLat = 90.0
	dotWidth := max(1, width*brailleCellWidth)
	dotHeight := max(1, height*brailleCellHeight)
	x := int(math.Round(((lon - minLon) / (maxLon - minLon)) * float64(dotWidth-1)))
	y := int(math.Round(((maxLat - lat) / (maxLat - minLat)) * float64(dotHeight-1)))
	return clampInt(x, 0, dotWidth-1), clampInt(y, 0, dotHeight-1)
}

func mapProjectCells(width, height int, lon, lat float64) (int, int) {
	dotX, dotY := mapProjectDots(width, height, lon, lat)
	return dotX / brailleCellWidth, dotY / brailleCellHeight
}

func mapProjectLineDots(width, height int, lon, lat float64) (int, int) {
	cellX, cellY := mapProjectCells(width, height, lon, lat)
	dotWidth := max(1, width*brailleCellWidth)
	dotHeight := max(1, height*brailleCellHeight)
	dotX := clampInt(cellX*brailleCellWidth+1, 0, dotWidth-1)
	dotY := clampInt(cellY*brailleCellHeight+1, 0, dotHeight-1)
	return dotX, dotY
}

type projectedPoint struct {
	x float64
	y float64
}

type lineSegment struct {
	a projectedPoint
	b projectedPoint
}

type linePath struct {
	segments []lineSegment
}

func mapProjectLineEndpoint(width, height int, lon, lat float64) projectedPoint {
	cellX, cellY := mapProjectCells(width, height, lon, lat)
	return projectedPoint{
		x: float64(cellX*brailleCellWidth) + 1.0,
		y: float64(cellY*brailleCellHeight) + 1.5,
	}
}

func mapProjectLinePoint(width, height int, lon, lat float64) projectedPoint {
	cellWidth := max(1, width)
	cellHeight := max(1, height)
	cellX := ((lon + 180.0) / 360.0) * float64(cellWidth-1)
	cellY := ((90.0 - lat) / 180.0) * float64(cellHeight-1)
	return projectedPoint{
		x: cellX*brailleCellWidth + 1.0,
		y: cellY*brailleCellHeight + 1.5,
	}
}

func (p *linePath) add(a, b projectedPoint) {
	p.segments = append(p.segments, lineSegment{a: a, b: b})
}

func trimSegment(segment lineSegment, trimStart, trimEnd float64) (lineSegment, bool) {
	dx := segment.b.x - segment.a.x
	dy := segment.b.y - segment.a.y
	length := math.Sqrt(dx*dx + dy*dy)
	if length <= 1e-6 || trimStart+trimEnd >= length {
		return lineSegment{}, false
	}
	unitX := dx / length
	unitY := dy / length
	return lineSegment{
		a: projectedPoint{
			x: segment.a.x + unitX*trimStart,
			y: segment.a.y + unitY*trimStart,
		},
		b: projectedPoint{
			x: segment.b.x - unitX*trimEnd,
			y: segment.b.y - unitY*trimEnd,
		},
	}, true
}

func (p *linePath) toCanvas(cellW, cellH int, style mapStyle) worldCanvas {
	raster := newBrailleRaster(cellW, cellH)
	for _, segment := range p.segments {
		dx := segment.b.x - segment.a.x
		dy := segment.b.y - segment.a.y
		steps := max(1, int(math.Ceil(math.Max(math.Abs(dx), math.Abs(dy))*8)))
		prevDotX := int(math.Round(segment.a.x - 0.5))
		prevDotY := int(math.Round(segment.a.y - 0.5))
		raster.paint(prevDotX, prevDotY, style, 3)
		for i := 1; i <= steps; i++ {
			t := float64(i) / float64(steps)
			x := segment.a.x + dx*t
			y := segment.a.y + dy*t
			dotX := int(math.Round(x - 0.5))
			dotY := int(math.Round(y - 0.5))
			raster.line(prevDotX, prevDotY, dotX, dotY, style, 3)
			prevDotX = dotX
			prevDotY = dotY
		}
	}
	return raster.toCanvas()
}

type geoVec3 struct {
	x float64
	y float64
	z float64
}

func geoToVec3(point geoPoint) geoVec3 {
	lonRad := point.Lon * math.Pi / 180.0
	latRad := point.Lat * math.Pi / 180.0
	cosLat := math.Cos(latRad)
	return geoVec3{
		x: cosLat * math.Cos(lonRad),
		y: cosLat * math.Sin(lonRad),
		z: math.Sin(latRad),
	}
}

func vec3ToGeo(vec geoVec3) geoPoint {
	length := math.Sqrt(vec.x*vec.x + vec.y*vec.y + vec.z*vec.z)
	if length == 0 {
		return geoPoint{}
	}
	x := vec.x / length
	y := vec.y / length
	z := vec.z / length
	return geoPoint{
		Lon: math.Atan2(y, x) * 180.0 / math.Pi,
		Lat: math.Asin(z) * 180.0 / math.Pi,
	}
}

func geoAngularDistance(a, b geoPoint) float64 {
	va := geoToVec3(a)
	vb := geoToVec3(b)
	dot := va.x*vb.x + va.y*vb.y + va.z*vb.z
	return math.Acos(math.Max(-1, math.Min(1, dot)))
}

func geoSlerp(a, b geoPoint, t float64) geoPoint {
	va := geoToVec3(a)
	vb := geoToVec3(b)
	dot := math.Max(-1, math.Min(1, va.x*vb.x+va.y*vb.y+va.z*vb.z))
	omega := math.Acos(dot)
	if omega < 1e-6 {
		return geoPoint{
			Lon: a.Lon + (b.Lon-a.Lon)*t,
			Lat: a.Lat + (b.Lat-a.Lat)*t,
		}
	}
	sinOmega := math.Sin(omega)
	scaleA := math.Sin((1-t)*omega) / sinOmega
	scaleB := math.Sin(t*omega) / sinOmega
	return vec3ToGeo(geoVec3{
		x: scaleA*va.x + scaleB*vb.x,
		y: scaleA*va.y + scaleB*vb.y,
		z: scaleA*va.z + scaleB*vb.z,
	})
}

func greatCirclePath(a, b geoPoint) []geoPoint {
	steps := max(24, int(math.Ceil(geoAngularDistance(a, b)*32)))
	points := make([]geoPoint, 0, steps+1)
	for i := 0; i <= steps; i++ {
		t := float64(i) / float64(steps)
		points = append(points, geoSlerp(a, b, t))
	}
	return points
}

func drawWrappedSegment(path *linePath, width, height int, a, b geoPoint) {
	lon0 := a.Lon
	lon1 := b.Lon
	if math.Abs(lon1-lon0) <= 180 {
		path.add(
			mapProjectLinePoint(width, height, lon0, a.Lat),
			mapProjectLinePoint(width, height, lon1, b.Lat),
		)
		return
	}

	if lon0 > 0 && lon1 < 0 {
		adjustedLon1 := lon1 + 360
		t := (180 - lon0) / (adjustedLon1 - lon0)
		crossLat := a.Lat + (b.Lat-a.Lat)*t
		path.add(
			mapProjectLinePoint(width, height, lon0, a.Lat),
			mapProjectLinePoint(width, height, 180, crossLat),
		)
		path.add(
			mapProjectLinePoint(width, height, -180, crossLat),
			mapProjectLinePoint(width, height, lon1, b.Lat),
		)
		return
	}

	if lon0 < 0 && lon1 > 0 {
		adjustedLon1 := lon1 - 360
		t := (-180 - lon0) / (adjustedLon1 - lon0)
		crossLat := a.Lat + (b.Lat-a.Lat)*t
		path.add(
			mapProjectLinePoint(width, height, lon0, a.Lat),
			mapProjectLinePoint(width, height, -180, crossLat),
		)
		path.add(
			mapProjectLinePoint(width, height, 180, crossLat),
			mapProjectLinePoint(width, height, lon1, b.Lat),
		)
		return
	}

	path.add(
		mapProjectLinePoint(width, height, lon0, a.Lat),
		mapProjectLinePoint(width, height, lon1, b.Lat),
	)
}

func drawGreatCircle(lines *linePath, width, height int, a, b geoPoint) {
	start := len(lines.segments)
	path := greatCirclePath(a, b)
	for i := 1; i < len(path); i++ {
		drawWrappedSegment(lines, width, height, path[i-1], path[i])
	}
	added := len(lines.segments) - start
	if added <= 0 {
		return
	}
	lines.segments[start].a = mapProjectLineEndpoint(width, height, a.Lon, a.Lat)
	last := len(lines.segments) - 1
	lines.segments[last].b = mapProjectLineEndpoint(width, height, b.Lon, b.Lat)
	const endpointTrim = 0.9
	if trimmed, ok := trimSegment(lines.segments[start], endpointTrim, 0); ok {
		lines.segments[start] = trimmed
	}
	if trimmed, ok := trimSegment(lines.segments[last], 0, endpointTrim); ok {
		lines.segments[last] = trimmed
	}
}

func placeRegionLabels(canvas *worldCanvas, regions []regionSummary) {
	for _, region := range regions {
		x, y := mapProjectCells(canvas.w, canvas.h, region.Lon, region.Lat)
		label := region.Code
		labelY := clampInt(y-1, 0, max(0, canvas.h-1))
		switch {
		case x+len(label)+2 < canvas.w:
			canvas.text(x+1, labelY, label, mapLabel)
		case x-len(label)-1 >= 0:
			canvas.text(x-len(label), labelY, label, mapLabel)
		default:
			canvas.text(clampInt(x-len(label)/2, 0, max(0, canvas.w-len(label))), clampInt(y+1, 0, max(0, canvas.h-1)), label, mapLabel)
		}
	}
}

func placeRegionMarkers(canvas *worldCanvas, regions []regionSummary, blinkOn bool) {
	for _, region := range regions {
		x, y := mapProjectCells(canvas.w, canvas.h, region.Lon, region.Lat)
		style := mapPeer
		if region.IsCurrent {
			style = mapCurrent
		}
		if blinkOn {
			canvas.set(x, y, '◉', style)
		} else {
			canvas.set(x, y, '◎', style)
		}
	}
}

func renderWorldMap(width, height int, regions []regionSummary, blinkOn bool) string {
	landRaster := newBrailleRaster(width, height)
	for _, point := range aboutWorldHighPoints {
		dotX, dotY := mapProjectDots(width, height, point.Lon, point.Lat)
		landRaster.paint(dotX, dotY, mapLand, 2)
	}
	var lines linePath

	var current *regionSummary
	for i := range regions {
		if regions[i].IsCurrent {
			current = &regions[i]
			break
		}
	}
	if current != nil {
		for _, region := range regions {
			if region.Code == current.Code {
				continue
			}
			drawGreatCircle(
				&lines,
				width,
				height,
				geoPoint{Lon: current.Lon, Lat: current.Lat},
				geoPoint{Lon: region.Lon, Lat: region.Lat},
			)
		}
	}
	canvas := landRaster.toCanvas()
	overlayCanvas(&canvas, lines.toCanvas(width, height, mapLine))
	placeRegionMarkers(&canvas, regions, blinkOn)
	placeRegionLabels(&canvas, regions)

	styles := map[mapStyle]lipgloss.Style{
		mapLand:    lipgloss.NewStyle().Foreground(theme.Surface),
		mapLine:    lipgloss.NewStyle().Foreground(theme.Accent),
		mapPeer:    lipgloss.NewStyle().Foreground(theme.Accent).Bold(true),
		mapCurrent: lipgloss.NewStyle().Foreground(theme.Primary).Bold(true),
		mapLabel:   lipgloss.NewStyle().Foreground(theme.Text).Bold(true),
	}
	return canvas.render(styles)
}
