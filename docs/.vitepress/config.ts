import { defineConfig } from "vitepress";
import path from "path";
import { readdirSync, readFileSync } from "fs";
import { parse as parseYaml } from "yaml";

const pathToContent = path.resolve("content");

interface SidebarFrontmatter {
  // per-page sidebar controls, e.g.:
  //   ---
  //   sidebar:
  //     order: 1        # lower = higher up; unset sorts to the end
  //     text: Custom    # overrides the H1 as the sidebar label
  //     hidden: true    # omit the page from the sidebar entirely
  //   ---
  order?: number;
  text?: string;
  hidden?: boolean;
}

interface Frontmatter {
  title?: string;
  sidebar?: SidebarFrontmatter | boolean;
}

// Split leading `---\n...\n---` frontmatter off a file and parse it as YAML.
function parseFrontmatter(content: string): Frontmatter {
  const match = content.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!match) return {};
  const data = parseYaml(match[1]);
  return data && typeof data === "object" ? (data as Frontmatter) : {};
}

// auto sidebar items gen
function getSidebarItems() {
  const items: { text: string; link: string; order: number }[] = [];
  const files = readdirSync(pathToContent).filter(
    (file) => file !== "index.md" && file.endsWith(".md"),
  );
  for (const file of files) {
    const filePath = path.join(pathToContent, file);
    const content = readFileSync(filePath, { encoding: "utf-8" });
    const fm = parseFrontmatter(content);

    // `sidebar: false` or `sidebar.hidden: true` drops the page.
    const sidebar = typeof fm.sidebar === "object" ? fm.sidebar : {};
    if (fm.sidebar === false || sidebar.hidden) continue;

    // label priority: sidebar.text → frontmatter title → first H1
    const h1 = content.match(/^#\s+(.*)$/m)?.[1];
    const text = sidebar.text ?? fm.title ?? h1;
    if (!text) continue;

    items.push({
      text,
      link: `/${file.replace(/\.md$/, "")}`,
      order: sidebar.order ?? Number.POSITIVE_INFINITY,
    });
  }

  // sort by explicit order, then alphabetically by label as a stable fallback
  items.sort((a, b) => a.order - b.order || a.text.localeCompare(b.text));
  return items.map(({ text, link }) => ({ text, link }));
}

export default defineConfig({
  srcDir: "content",

  title: "SM Manager",
  description: "SM Manager Docs...",
  cleanUrls: true,

  themeConfig: {
    sidebar: [
      {
        text: "Examples",
        items: getSidebarItems(),
      },
    ],

    search: {
      provider: "local",
    },

    socialLinks: [
      { icon: "github", link: "https://github.com/supportmailapp/sm-manager" },
    ],
  },
});
