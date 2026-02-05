#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""
Embed files as C arrays for inclusion in the REST server binary.
This allows the Swagger UI and OpenAPI spec to be served without
external file dependencies.
"""

import sys
import os


def sanitize_name(filename):
    """Convert filename to valid C identifier."""
    name = os.path.basename(filename)
    # Replace dots and hyphens with underscores
    name = name.replace('.', '_').replace('-', '_')
    return name


def embed_file(filepath, varname):
    """Read file and return C array definition."""
    with open(filepath, 'rb') as f:
        data = f.read()

    lines = []
    lines.append(f"const unsigned char {varname}[] = {{")

    # Format bytes as hex, 16 per line
    for i in range(0, len(data), 16):
        chunk = data[i:i + 16]
        hex_values = ', '.join(f'0x{b:02x}' for b in chunk)
        lines.append(f"    {hex_values},")

    lines.append("};")
    lines.append(f"const unsigned int {varname}_len = {len(data)};")
    lines.append("")

    return '\n'.join(lines)


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <swagger_dir> <openapi_json> <output.c>",
              file=sys.stderr)
        sys.exit(1)

    swagger_dir = sys.argv[1]
    openapi_json = sys.argv[2]
    output_file = sys.argv[3]

    files_to_embed = [
        (os.path.join(swagger_dir, 'index.html'), 'swagger_index_html'),
        (os.path.join(swagger_dir, 'swagger-ui-bundle.min.js'),
         'swagger_ui_bundle_min_js'),
        (os.path.join(swagger_dir, 'swagger-ui-standalone-preset.min.js'),
         'swagger_ui_standalone_preset_min_js'),
        (os.path.join(swagger_dir, 'swagger-ui.min.css'), 'swagger_ui_min_css'),
        (openapi_json, 'openapi_json'),
    ]

    output_lines = []
    output_lines.append("// SPDX-FileCopyrightText: 2025-2026 "
                        "Chimera-NAS Project Contributors")
    output_lines.append("//")
    output_lines.append("// SPDX-License-Identifier: LGPL-2.1-only")
    output_lines.append("")
    output_lines.append("// Auto-generated file - do not edit")
    output_lines.append("")

    for filepath, varname in files_to_embed:
        if not os.path.exists(filepath):
            print(f"Error: File not found: {filepath}", file=sys.stderr)
            sys.exit(1)

        output_lines.append(f"/* Embedded: {os.path.basename(filepath)} */")
        output_lines.append(embed_file(filepath, varname))

    with open(output_file, 'w') as f:
        f.write('\n'.join(output_lines))

    print(f"Generated {output_file}")


if __name__ == '__main__':
    main()
