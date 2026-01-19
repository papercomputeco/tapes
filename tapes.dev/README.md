# tapes.dev

Landing page for [Tapes](https://github.com/papercomputeco/tapes) - built with [Astro](https://astro.build).

**Live site:** https://tapes.dev

## Setup

```sh
cd tapes.dev
npm install
```

## Development

```sh
npm run dev
```

Opens at http://localhost:4321

## Build

```sh
npm run build
```

Output is in `./dist/`.

Preview the build locally:

```sh
npm run preview
```

## Project Structure

```
tapes.dev/
├── public/
│   ├── favicon.svg
│   ├── robots.txt
│   ├── sitemap.xml
│   └── tapes-social.png    # OG image for social sharing
├── src/
│   ├── layouts/
│   │   └── Layout.astro    # Base layout with SEO meta tags
│   └── pages/
│       └── index.astro     # Landing page content
├── astro.config.mjs
└── package.json
```

## Deployment

Hosted on Netlify. Deploys automatically on push to `main`, or manually:

```sh
netlify deploy --prod
```

## Features

- Dark/light theme toggle (respects system preference)
- Copy-to-clipboard for code blocks
- SEO optimized (OG tags, Twitter cards, JSON-LD schema)
