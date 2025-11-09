# IOMETE Autoloader - Frontend Design

## Overview
This document outlines the high-level UI elements needed for the IOMETE Autoloader, a zero-code data ingestion system.

Important: Only design just plain html. I need to see the UI that can run as plan html on
my local.

---

## 1. Main Dashboard / Ingestions List

**Purpose:** Central view showing all configured ingestions

**Key Elements:**
- Table/list of ingestions with:
  - Name, source type (S3/Azure/GCS), status, schedule
  - Last run time, next scheduled run
  - Quick actions (pause/resume, edit, delete)
- Filter/search by status, source type, name
- "Create New Ingestion" button
- Status badges (Active, Paused, Draft, Error)
- Metrics summary: total ingestions, active runs, success rate

---

## 2. Ingestion Configuration Wizard

**Purpose:** Step-by-step form to create/edit ingestion

**Steps:**

### Step 1: Basic Information
- Ingestion name
- Description (optional)
- Source type selector (AWS S3 / Azure Blob / GCS)

### Step 2: Source Configuration
- Cloud credentials input (varies by source type)
- Bucket/container name
- Path prefix/pattern
- File format selection (JSON, CSV, Parquet, etc.)

### Step 3: Destination
- Database selection
- Table name input
- Schema inference option
- Partition configuration (optional)

### Step 4: Schedule
- Schedule type picker (Hourly, Daily, Custom Cron)
- Cron expression builder/input
- Timezone selector

### Step 5: Advanced Options
- Processing mode (append, overwrite)
- Schema evolution settings
- Resource allocation
- Checkpoint configuration

### Step 6: Review & Preview
- Summary of all configurations
- Cost estimation display
- "Test Configuration" button
- Preview results table (sample data)
- "Activate" / "Save as Draft" buttons

---

## 3. Run History View

**Purpose:** Show execution history for an ingestion

**Key Elements:**
- Timeline/table of runs with:
  - Run ID, start/end time, duration
  - Status (Running, Completed, Failed, Cancelled)
  - Records processed, files processed
  - Error messages (if failed)
- Filter by date range, status
- Retry button for failed runs
- Real-time updates for running jobs
- Download logs option
- Metrics charts (records over time, success rate)

---

## 4. Ingestion Detail Page

**Purpose:** Comprehensive view of a single ingestion

**Sections:**
- Header with name, status, quick actions
- Configuration summary (read-only cards)
- Latest run status widget
- Recent runs table (mini version of run history)
- Schema information display
- Performance metrics
- Edit/pause/resume/delete actions

---

## 5. Cluster Configuration

**Purpose:** Configure Spark cluster connection

**Key Elements:**
- Cluster endpoint input
- Connection test button
- Status indicator (connected/disconnected)
- Cluster health metrics
- Resource availability display

---

## 6. Preview/Test Mode

**Purpose:** Test configuration before activation

**Key Elements:**
- File discovery results (list of files found)
- Sample data table (first N rows from discovered files)
- Schema preview (inferred structure)
- Validation warnings/errors
- File count and size estimations
- "Looks good, activate" button

---

## 7. Cost Estimation Widget

**Purpose:** Display estimated costs before activation

**Key Elements:**
- Estimated cost per run
- Estimated monthly cost (based on schedule)
- Breakdown: compute cost, storage cost
- Data volume indicators
- Comparison: current vs projected costs

---

## 8. Monitoring Dashboard (Optional Enhancement)

**Purpose:** Real-time monitoring of active runs

**Key Elements:**
- Active runs widget with progress bars
- Live logs/events stream
- Resource utilization charts
- Error alerts panel
- Quick jump to run details

---

## 9. Schema Evolution View

**Purpose:** Review and approve schema changes

**Key Elements:**
- Side-by-side schema comparison (old vs new)
- Highlighted changes (new columns, type changes)
- Impact analysis
- Approve/reject actions
- Auto-apply option toggle

---

## 10. Common UI Components

**Reusable Elements:**
- Status badges (color-coded)
- Action buttons (pause, resume, edit, delete, retry)
- Loading spinners/progress indicators
- Error/success notifications (toasts)
- Confirmation modals
- Date/time pickers
- Cron expression builder
- Breadcrumb navigation
- Search/filter bars
- Data tables with sorting/pagination

---

## UI/UX Principles

- **Wizard-based onboarding:** Step-by-step for complex configuration
- **Progressive disclosure:** Show advanced options only when needed
- **Real-time feedback:** Immediate validation and status updates
- **Error prevention:** Validate before activation, show warnings
- **Visibility:** Clear status indicators and progress tracking
- **Consistency:** Use design system/component library
- **Responsive:** Support desktop and tablet views

---

## Technology Stack Suggestions

- **Framework:** React or Vue.js
- **UI Library:** Material-UI, Ant Design, or Chakra UI
- **State Management:** Redux, Zustand, or Pinia
- **Data Fetching:** React Query or SWR
- **Forms:** Formik/React Hook Form or VeeValidate
- **Charts:** Recharts, Chart.js, or Apache ECharts
- **Tables:** TanStack Table (React Table)
- **Date/Time:** date-fns or Day.js
- **Cron:** react-js-cron or vue-cron-editor

---

## Next Steps

1. **Detailed wireframes** for each screen
2. **Component breakdown** and design system
3. **API integration** contracts (based on existing FastAPI endpoints)
4. **User flows** and interaction patterns
5. **Responsive design** specifications
6. **Accessibility** considerations
