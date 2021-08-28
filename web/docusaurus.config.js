const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'MAGDA',
  tagline: 'Library for building Modular and Asynchronous Graphs with Directed and Acyclic edges',
  url: 'https://NeuroSYS-pl.github.io',
  baseUrl: '/magda/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  // TODO: Add favicon
  // favicon: 'img/favicon.ico',
  organizationName: 'NeuroSYS-pl',
  projectName: 'magda',
  themeConfig: {
    navbar: {
      title: 'MAGDA 👧',
      // TODO: Add logo
      // logo: {
      //   alt: 'MAGDA Logo',
      //   src: 'img/logo.svg',
      // },
      items: [
        {
          type: 'doc',
          docId: 'home',
          position: 'left',
          label: 'Guides',
        },
        {
          href: 'https://github.com/NeuroSYS-pl/magda/tree/main/examples',
          position: 'left',
          label: 'Examples',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
        },
        {
          href: 'https://github.com/NeuroSYS-pl/magda',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      logo: {
        alt: 'NeuroSYS',
        src: 'https://neurosys.com/wp-content/themes/neurosys/static/images/logo-white.svg',
        href: 'https://neurosys.com/',
      },
      copyright: `Copyright © ${new Date().getFullYear()} NeuroSYS, Built with Docusaurus.`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/NeuroSYS-pl/magda/edit/main/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
