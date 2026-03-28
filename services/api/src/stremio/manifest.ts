export function getManifest(baseUrl: string) {
  return {
    id: 'cc.plaio.streamdb',
    version: '0.1.0',
    name: 'StreamDB',
    description: 'The perfect stream, every time. Personalized quality matching with verified file metadata.',
    logo: 'https://i.imgur.com/6dQlPhb.png',
    resources: [
      { name: 'stream', types: ['movie', 'series'], idPrefixes: ['tt'] },
    ],
    types: ['movie', 'series'],
    catalogs: [],
    idPrefixes: ['tt'],
    behaviorHints: {
      configurable: true,
      configurationRequired: true,
    },
  };
}
