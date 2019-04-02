module.exports = {
    title: 'Pragmatias Documentation',
    description: 'Wiki / Documentation / Project / Stuff',
    base: '/',

    themeConfig: {
        nav: [
            { text: 'Home', link: '/' },
            { text: 'Contact', link: '/contact/' },
            { text: 'Repository', link: 'https://github.com/pragmatias' }
        ],
        sidebar: [
            ['/wiki/','Wiki'],
            ['/wiki/opensuse','OpenSuse'],
        ],
        search: true,
        searchMaxSuggestions: 20,
        lastUpdated: 'string',
    }

};

