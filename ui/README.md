# IOMETE Autoloader UI - Plain HTML Prototype

This directory contains plain HTML/CSS prototypes for the IOMETE Autoloader user interface, designed according to the IOMETE Console Design System.

## Files Overview

### Core Files
- **`iomete-styles.css`** - Complete CSS implementing the IOMETE design system with all colors, typography, components, and utilities

### HTML Pages
- **`index.html`** - Main dashboard showing all ingestions with filtering and search
- **`wizard.html`** - 6-step wizard for creating new ingestions
- **`detail.html`** - Detailed view of a single ingestion with configuration and stats
- **`runs.html`** - Run history view with execution timeline and logs

## How to Use

### Quick Start
1. Open `index.html` in your web browser to see the main dashboard
2. Navigate between pages using the links and buttons

### Running Locally
Simply open any HTML file directly in your browser:
```bash
# On macOS
open ui/index.html

# On Linux
xdg-open ui/index.html

# On Windows
start ui/index.html
```

Or use a simple HTTP server:
```bash
# Python 3
cd ui
python3 -m http.server 8000

# Then open: http://localhost:8000
```

## Design System Features

### Color Palette
Following IOMETE Console design system:
- **Stone** (Warm Grays) - Primary neutral colors
- **Lake** (Blue) - Primary actions, links
- **Racing** (Green) - Success states
- **Reef** (Coral/Red) - Error states
- **Apricot** (Orange) - Warning states

### Typography
- **Font**: Inter font family
- **Sizes**: 12px (xs), 14px (sm), 16px (base), 18px (lg)
- **Weights**: 400 (normal), 500 (medium emphasis), 700 (strong emphasis)

### Components
All components follow Ant Design 5.x patterns:
- Buttons (primary, secondary, text, danger, success)
- Cards with headers and sections
- Tables with sorting and filtering
- Forms with validation
- Tags/Badges for status
- Modals and overlays
- Wizard/Stepper navigation
- Timeline components

### Responsive Design
- Mobile-first approach
- Breakpoints at 768px
- Flexible grid layouts
- Collapsible navigation on small screens

## Page Descriptions

### 1. Main Dashboard (`index.html`)
**Features:**
- Statistics cards showing key metrics
- Filterable table of all ingestions
- Search functionality
- Status badges (Active, Paused, Draft, Error, Running, Failed)
- Quick actions for each ingestion
- Pagination

**Interactive Elements:**
- Search bar filters table rows
- Status and source dropdowns filter data
- Modal confirmation for pause action
- Links to detail and wizard pages

### 2. Ingestion Wizard (`wizard.html`)
**6-Step Configuration Flow:**

**Step 1: Basic Information**
- Ingestion name and description
- Source type selection (AWS S3, Azure Blob, GCS)
- Radio card UI for cloud provider selection

**Step 2: Source Configuration**
- Bucket name and path prefix
- File format selection (JSON, CSV, Parquet, Avro, ORC)
- Collapsible AWS credentials section
- Security info banner

**Step 3: Destination**
- Database and table name
- Auto-schema inference option
- Partition columns configuration
- Apache Iceberg info banner

**Step 4: Schedule**
- Schedule type (Hourly, Daily, Custom Cron)
- Time picker for daily schedules
- Cron expression input
- Timezone selection

**Step 5: Advanced Options**
- Processing mode (Append/Overwrite)
- Schema evolution settings
- Spark configuration JSON

**Step 6: Review & Activate**
- Configuration summary
- Cost estimation (per run, monthly)
- Data preview table
- Test configuration button

**Interactive Elements:**
- Step indicator shows progress
- Previous/Next navigation
- Form validation
- Dynamic review updates

### 3. Ingestion Detail Page (`detail.html`)
**Features:**
- Latest run status with metrics
- Comprehensive configuration display
- Recent runs mini-table
- Quick stats sidebar:
  - Total runs
  - Success rate with progress bar
  - Total data processed
  - Average duration
- Schedule information
- Schema preview
- Cost overview

**Layout:**
- Two-column layout (main content + sidebar)
- Action buttons (Run Now, Pause, Edit, Delete)
- Status tags and badges
- Progress visualization

### 4. Run History (`runs.html`)
**Features:**
- Complete execution history table
- Metrics summary cards
- Date and status filtering
- Run details panel (sticky sidebar)
- Execution timeline
- Live log viewer
- Retry failed runs

**Interactive Elements:**
- Click row to show details in sidebar
- Timeline visualization
- Styled log viewer with color-coded log levels
- Download logs functionality
- Pagination

**Log Viewer:**
- Dark terminal-style UI
- Color-coded log levels (INFO, WARN, ERROR, SUCCESS)
- Scrollable with max height
- Monospace font for readability

## Customization

### Colors
Edit color variables in `iomete-styles.css` under the `:root` section:
```css
:root {
  --color-primary: #202020;
  --color-success: #32b77f;
  --color-error: #e43920;
  /* ... etc */
}
```

### Dark Mode
Dark mode color overrides are defined under `[data-theme='dark']`:
```css
[data-theme='dark'] {
  --color-fill-container: #141414;
  --color-text: rgba(255, 255, 255, 0.85);
  /* ... etc */
}
```

To enable dark mode, add `data-theme="dark"` to the `<html>` tag.

### Components
All component styles are modular and can be reused. Key component classes:
- `.card`, `.card-header`, `.card-small`, `.card-large`
- `.btn`, `.btn-primary`, `.btn-secondary`, `.btn-text`
- `.tag`, `.tag-active`, `.tag-failed`, etc.
- `.stat-card`, `.stats-grid`
- `.wizard`, `.wizard-steps`, `.wizard-content`
- `.table-container`, `table`, `thead`, `tbody`

## Browser Compatibility

Tested and works on:
- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)

Uses modern CSS features:
- CSS Grid
- CSS Variables (Custom Properties)
- Flexbox
- CSS Animations

## Next Steps

To convert these to a real application:

1. **React/Vue Integration**
   - Convert HTML to component templates
   - Add state management
   - Implement API calls

2. **Backend Integration**
   - Connect to FastAPI endpoints
   - Real data fetching
   - Form submission handlers

3. **Enhanced Features**
   - Real-time updates via WebSocket
   - Advanced filtering and sorting
   - Chart visualizations
   - File upload handling

4. **Production Optimizations**
   - Build process with bundling
   - CSS preprocessing (SCSS)
   - Code splitting
   - Performance optimization

## Design Reference

These prototypes follow:
- **Design Document**: `docs/ui/frontend-design.md`
- **Style Guide**: `.claude/skills/frontend-designer/style.md`
- **IOMETE Console Design System**: Tailwind CSS v4 + Ant Design 5.x

## Questions or Issues?

For design system questions, refer to the style guide at `.claude/skills/frontend-designer/style.md` which contains comprehensive documentation on:
- Complete color palette
- Typography scale
- Component patterns
- Spacing system
- Best practices
