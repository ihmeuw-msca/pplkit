# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import datetime
import tomllib

with open("../pyproject.toml", "rb") as f:
    about = tomllib.load(f)["project"]


# -- Project information -----------------------------------------------------

project = about["name"]
author = ", ".join([info["name"] for info in about["authors"]])
copyright = f"2024-{datetime.datetime.today().year}, {author}"


# The full version, including alpha/beta/rc tags
version = about["version"]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]
autodoc_typehints = "none"
napoleon_use_rtype = False
typehints_use_rtype = False
autodoc_member_order = "bysource"
autodoc_type_aliases = {
    "ArrayLike": "ArrayLike",
    "NDArray": "NDArray",
    "DataFrame": "DataFrame",
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"
pygments_style = "catppuccin-latte"
pygments_dark_style = "catppuccin-macchiato"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_title = f"{project} {version}"
html_theme_options = {
    "sidebar_hide_name": False,
    "light_logo": "logo/logo-light.png",
    "dark_logo": "logo/logo-dark.png",
    # Catppuccin Latte
    "light_css_variables": {
        "color-brand-primary": "#1e66f5",
        "color-brand-content": "#1e66f5",
        "color-problematic": "#d20f39",
        "color-foreground-primary": "#4c4f69",
        "color-foreground-secondary": "#5c5f77",
        "color-foreground-muted": "#6c6f85",
        "color-foreground-border": "#9ca0b0",
        "color-background-primary": "#eff1f5",
        "color-background-secondary": "#e6e9ef",
        "color-background-hover": "#ccd0da",
        "color-background-hover--transparent": "#ccd0da00",
        "color-background-border": "#bcc0cc",
        "color-background-item": "#dce0e8",
        "color-highlight-on-target": "#df8e1d33",
        "color-admonition-background": "#7287fd18",
        "color-card-border": "#bcc0cc",
        "color-card-background": "#e6e9ef",
        "color-card-marginals-background": "#dce0e8",
        "color-link": "#1e66f5",
        "color-link--hover": "#04a5e5",
        "color-link-underline": "#1e66f580",
        "color-link-underline--hover": "#04a5e5",
        "color-link--visited": "#df8e1d",
        "color-link-underline--visited": "#df8e1d80",
        "color-link--visited--hover": "#04a5e5",
        "color-link-underline--visited--hover": "#04a5e5",
        "color-inline-code-background": "#e6e9ef",
        "color-inline-code-foreground": "#179299",
        "color-api-overall": "#8c8fa1",
        "color-api-name": "#df8e1d",
        "color-api-pre-name": "#df8e1d",
        "color-api-keyword": "#8839ef",
        "color-sidebar-link-text--top-level": "#4c4f69",
        "color-sidebar-brand-text": "#4c4f69",
        "color-sidebar-caption-text": "#5c5f77",
        "color-sidebar-search-foreground": "#4c4f69",
        "color-sidebar-search-border": "#bcc0cc",
        "color-sidebar-search-background": "#e6e9ef",
        "color-sidebar-search-background--focus": "#dce0e8",
        "color-sidebar-search-icon": "#6c6f85",
        "color-toc-title-text": "#5c5f77",
        "color-toc-item-text": "#6c6f85",
        "color-toc-item-text--hover": "#1e66f5",
        "color-toc-item-text--active": "#1e66f5",
    },
    # Catppuccin Macchiato
    "dark_css_variables": {
        "color-brand-primary": "#8aadf4",
        "color-brand-content": "#8aadf4",
        "color-problematic": "#ed8796",
        "color-foreground-primary": "#cad3f5",
        "color-foreground-secondary": "#b8c0e0",
        "color-foreground-muted": "#a5adcb",
        "color-foreground-border": "#6e738d",
        "color-background-primary": "#1e2030",
        "color-background-secondary": "#24273a",
        "color-background-hover": "#363a4f",
        "color-background-hover--transparent": "#363a4f00",
        "color-background-border": "#494d64",
        "color-background-item": "#363a4f",
        "color-highlight-on-target": "#eed49f33",
        "color-admonition-background": "#b7bdf818",
        "color-card-border": "#494d64",
        "color-card-background": "#24273a",
        "color-card-marginals-background": "#24273a",
        "color-link": "#8aadf4",
        "color-link--hover": "#91d7e3",
        "color-link-underline": "#8aadf480",
        "color-link-underline--hover": "#91d7e3",
        "color-link--visited": "#eed49f",
        "color-link-underline--visited": "#eed49f80",
        "color-link--visited--hover": "#91d7e3",
        "color-link-underline--visited--hover": "#91d7e3",
        "color-inline-code-background": "#24273a",
        "color-inline-code-foreground": "#8bd5ca",
        "color-api-overall": "#8087a2",
        "color-api-name": "#eed49f",
        "color-api-pre-name": "#eed49f",
        "color-api-keyword": "#c6a0f6",
        "color-sidebar-link-text--top-level": "#cad3f5",
        "color-sidebar-brand-text": "#cad3f5",
        "color-sidebar-caption-text": "#b8c0e0",
        "color-sidebar-search-foreground": "#cad3f5",
        "color-sidebar-search-border": "#494d64",
        "color-sidebar-search-background": "#24273a",
        "color-sidebar-search-background--focus": "#1e2030",
        "color-sidebar-search-icon": "#a5adcb",
        "color-toc-title-text": "#b8c0e0",
        "color-toc-item-text": "#a5adcb",
        "color-toc-item-text--hover": "#8aadf4",
        "color-toc-item-text--active": "#8aadf4",
    },
}
