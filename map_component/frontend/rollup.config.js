// rollup.config.js
import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'
import serve from 'rollup-plugin-serve'
import livereload from 'rollup-plugin-livereload'

export default {
  input: 'main.js',
  output: {
    file: 'build/bundle.js',
    format: 'iife',
    name: 'MapComponent'
  },
  plugins: [
    resolve(),
    commonjs(),
    // Serve files from 'build' and maybe a root index.html
    serve({
      open: false,
      verbose: true,
      contentBase: ['build', '.'], // or wherever index.html is
      host: 'localhost',
      port: 3000
    }),
    // Optional: refresh browser on changes
    livereload({
      watch: 'build',
    }),
  ]
}
