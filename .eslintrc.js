module.exports = {
  plugins: ['prettier'],
  extends: ['plugin:prettier/recommended'],
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module',
  },
  env: {
    node: true,
    es6: true,
    mocha: true,
  }
}
