# NDPify

This document exists to make UI work look and feel like the National Data Platform.

Use it when you want a product UI that is visually consistent with `nationaldataplatform.org`:

- light surfaces
- blue-first accents
- restrained spacing
- Lexend + Roboto typography
- clear utility-first information hierarchy
- conventional layout patterns

This is not a branding fantasy document. It is a practical UI contract.

If `Uncodixfy.md` tells you what to stop doing, this file tells you what to do instead when the target is NDP.

## Intent

The National Data Platform visual language is:

- functional, not theatrical
- bright, not beige
- structured, not playful
- modern, but not ornamental
- brand-aware, but not overbranded

The design should feel like an operational web product, not a landing page, not a startup dashboard mockup, and not a “premium dark console”.

## Core Style

- Backgrounds: light and crisp
- Surfaces: white or near-white
- Accent color: medium-to-bright blue
- Typography: Lexend for headings, Roboto for body text
- Radius: 8px to 10px for most UI
- Borders: always visible, light blue-gray
- Shadows: minimal or none
- Motion: barely noticeable, mostly hover/focus color changes
- Layout: predictable sidebar + content or standard page sections

## Primary Tokens

Use these as defaults unless the product already has stronger local tokens.

```css
:root {
  --ndp-bg: #f4f8fc;
  --ndp-surface: #ffffff;
  --ndp-surface-alt: #f7fbff;
  --ndp-surface-soft: #eef5fb;
  --ndp-line: #d7e3ef;
  --ndp-line-strong: #bdd0e4;
  --ndp-text: #14263a;
  --ndp-text-muted: #63788f;
  --ndp-primary: #0071ff;
  --ndp-primary-hover: #0885ff;
  --ndp-primary-soft: #edf5ff;
  --ndp-primary-soft-strong: #dff0ff;
  --ndp-danger: #c2574c;
  --ndp-danger-soft: #fff1ef;
}
```

## Typography

- Headings: `Lexend`
- Body/UI text: `Roboto`
- Avoid decorative mixed font systems
- Use clear hierarchy instead of oversized display text

Recommended:

```css
font-family: "Roboto", sans-serif;
```

For headings:

```css
font-family: "Lexend", sans-serif;
```

### Sizing

- `h1`: 28px to 32px
- `h2`: 16px to 20px
- Body: 14px to 15px
- Labels: 13px to 14px
- Meta text: 12px to 13px

### Weight

- headings: `600`
- primary UI labels: `500`
- body: `400`
- do not use excessive `700` everywhere

## Layout Rules

### Sidebar

- Fixed width: `240px` to `256px`
- Solid background
- Single border-right
- No floating outer shell
- No giant logo tile
- No KPI blocks inside sidebar unless functionally necessary

### Header / Toolbar

- Simple title and short supporting sentence
- Action buttons aligned right
- No hero section
- No eyebrow labels
- No promotional copy

### Content Sections

- Use normal cards or bordered sections
- Padding: `20px` to `24px`
- Gap between sections: `16px` to `20px`
- Keep grids balanced and readable

## Component Standards

### Buttons

- Radius: `8px`
- Solid primary blue for primary actions
- Outline buttons for secondary actions
- No gradients
- No pill shape
- No glow

Primary button:

```css
background: #0071ff;
border-color: #0071ff;
```

Hover:

```css
background: #0885ff;
border-color: #0885ff;
```

### Inputs

- White background
- Visible blue-gray border
- Radius: `8px`
- Focus ring: subtle blue
- No floating label behavior

### Cards / Sections

- White surface
- Light border
- Radius: `10px`
- Minimal shadow or none
- Avoid dark panels unless the product specifically needs code/output contrast

### Lists

- Bordered container
- Standard row separators
- Hover is optional and subtle
- No decorative row ornaments

### Chips / Tags

- Use only when functional
- Background should be pale blue
- Border should be visible
- Radius: `8px`
- Never use giant rounded pills

### Result / Code Panels

- Allowed to be slightly tinted or darker for contrast
- Still keep border and radius restrained
- Do not turn the entire UI into a dark theme because one code block exists

## Navigation

- Links should be plain and clear
- Active or hover state should rely on blue text and pale blue background
- No animated sliding indicators
- No transform-on-hover
- No badges unless they reflect real counts or system state

## Spacing Scale

Use a stable spacing rhythm:

- `4px`
- `8px`
- `12px`
- `16px`
- `20px`
- `24px`
- `32px`

Do not invent odd spacing values unless needed for alignment.

## Borders and Shadows

### Borders

- Always prefer borders over dramatic shadows for structure
- Border color should be blue-gray, not neutral muddy gray

### Shadows

- Keep shadows minimal
- Avoid anything larger than a low-elevation card shadow
- If border is enough, skip shadow entirely

Recommended:

```css
box-shadow: none;
```

or at most:

```css
box-shadow: 0 2px 8px rgba(20, 38, 58, 0.06);
```

## Backgrounds

- Use soft light backgrounds
- A subtle top-to-bottom gradient is acceptable
- No radial glows
- No glassmorphism
- No neon overlays
- No abstract decorative blobs

Acceptable:

```css
background: linear-gradient(180deg, #f7fbff 0%, #f4f8fc 48%, #eef4fa 100%);
```

## Motion

- Use `140ms` to `180ms` transitions
- Limit to color, background, border-color
- No bounce
- No scale transforms
- No translate hover effects

## NDP Look Checklist

If the screen matches these points, you are close:

- It feels brighter and more open than a typical internal dashboard
- Blue is the dominant accent
- The UI looks like a serious web platform, not a SaaS template
- Sections are obvious without heavy decoration
- Typography is clean and modern
- Controls feel standard and reliable
- Nothing is trying too hard

## Hard No

- No dark dashboard look unless explicitly required
- No giant rounded corners
- No pill UI as the primary shape language
- No purple-first palette
- No blurred glass panels
- No radial background glow
- No giant hero strip inside an app
- No serif headlines
- No premium-marketing copy inside product screens
- No status dots and ornaments everywhere
- No oversized shadows
- No vague “control center” language
- No random accent colors outside the blue family
- No neon cyan
- No decorative gradients on buttons
- No fake analytics cards just to fill space

## Brand Placement

If using an NDP logo:

- keep it modest
- place it in a top-left lockup or product header
- do not put it inside a decorative badge
- do not surround it with colored effects
- let whitespace do the work

## Example Structure

This is the kind of structure to prefer:

```html
<div class="app-shell">
  <aside class="sidebar">
    <div class="brand-lockup">
      <img alt="National Data Platform" />
      <div class="brand">GNSS Bridge</div>
    </div>
    <nav class="sidebar-nav">
      <a href="#access">Access</a>
      <a href="#stations">Stations</a>
      <a href="#streams">Streams</a>
    </nav>
  </aside>

  <main class="workspace">
    <header class="toolbar">
      <div>
        <h1>Derived streams</h1>
        <p>Configure access, choose stations, and manage active sessions.</p>
      </div>
      <div class="toolbar-actions">
        <button>Refresh</button>
        <button>Stop all</button>
      </div>
    </header>

    <section class="section-card">...</section>
    <section class="section-card">...</section>
    <section class="section-card">...</section>
  </main>
</div>
```

## Rule

If a design choice makes the screen feel more like a clean public-sector or research platform, keep it.

If it makes the screen feel more like an AI-generated startup dashboard, remove it.
