const fs = require('fs');
const path = require('path');

const distDir = path.resolve(__dirname, '../lib/esm');

function addFileExtensions(dir) {
	const files = fs.readdirSync(dir);
	for (const file of files) {
		const fullPath = path.join(dir, file);
		if (fs.statSync(fullPath).isDirectory()) {
			addFileExtensions(fullPath);
		} else if (fullPath.endsWith('.js')) {
			let content = fs.readFileSync(fullPath, 'utf-8');
			content = content.replace(
				/import\s+(.+?)\s+from\s+['"](.+?)['"]/g,
				(match, imports, source) => {
					if (!source.endsWith('.js') && !source.startsWith('./') && !source.startsWith('../')) {
						return match;
					}
					return `import ${imports} from '${source}.js'`;
				}
			);
			fs.writeFileSync(fullPath, content, 'utf-8');
		}
	}
}

addFileExtensions(distDir);
