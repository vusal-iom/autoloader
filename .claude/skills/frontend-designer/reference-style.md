# IOMETE Console Design System & Style Guide

> Comprehensive style guide for maintaining design consistency across the IOMETE Console application

## Table of Contents

- [Overview](#overview)
- [Color Palette](#color-palette)
- [Typography](#typography)
- [Spacing System](#spacing-system)
- [Component Styles](#component-styles)
- [Shadows & Elevation](#shadows--elevation)
- [Animations & Transitions](#animations--transitions)
- [Border Radius](#border-radius)
- [Opacity & Transparency](#opacity--transparency)
- [Common Tailwind CSS Usage](#common-tailwind-css-usage)
- [Example Component Reference](#example-component-reference)

---

## Overview

The IOMETE Console design system is built on:
- **Tailwind CSS v4** for utility-first styling
- **Ant Design 5.x** for component foundation
- **CSS Variables** for theme-aware colors
- **Inter font family** for typography

### Design Philosophy

1. **Consistency First**: Use Ant Design Flex over custom Tailwind flex for consistent spacing
2. **Theme-Aware**: All colors use CSS variables to support light/dark modes
3. **Semantic Naming**: Colors have semantic names (stone, lake, reef) rather than generic names
4. **Minimal Overrides**: Prefer Ant Design defaults, override only when necessary
5. **Accessibility**: Maintain WCAG AA contrast ratios

---

## Color Palette

### Brand Colors

#### Stone (Primary Neutral - Warm Grays)
```css
--color-stone-50: #f2eeeb    /* Light backgrounds, sidebar */
--color-stone-100: #e8e1da   /* Subtle fills */
--color-stone-200: #d6cac1   /* Borders, dividers */
--color-stone-300: #c9b9ad   /* Tag backgrounds */
--color-stone-400: #b0a097
--color-stone-500: #9e8b82   /* Medium emphasis */
--color-stone-600: #806e66
--color-stone-700: #6b5851   /* Text on light backgrounds */
--color-stone-800: #54433e
--color-stone-900: #473733   /* Strong emphasis */
```

**Usage:**
- `stone-50`: Sidebar backgrounds, secondary surfaces
- `stone-300`: Tag backgrounds (medium style)
- `stone-700`: Primary text on light surfaces

#### Lake (Blue Tones)
```css
--color-lake-50: #e8f3fd
--color-lake-100: #dce9fa
--color-lake-200: #cbdcf8
--color-lake-300: #b1c4f2    /* Links, info states */
--color-lake-400: #96a7eb
--color-lake-500: #8292e0    /* Primary blue */
--color-lake-600: #707ecf
--color-lake-700: #565f9c
--color-lake-800: #424778
--color-lake-900: #373a63    /* Dark blue backgrounds */
```

**Usage:**
- `lake-300`: Links, info tags
- `lake-500`: Primary blue actions
- `lake-900`: Dark theme backgrounds

#### Reef (Coral/Red - Error/Danger)
```css
--color-reef-50: #faf2f0
--color-reef-100: #fae1dc
--color-reef-200: #fad0c8
--color-reef-300: #f5beb3
--color-reef-400: #fa9884
--color-reef-500: #ed7e68    /* Error/danger states */
--color-reef-600: #cf543c
--color-reef-700: #b03e27
--color-reef-800: #96311d
--color-reef-900: #752414
```

**Usage:**
- `reef-500`: Error messages, failed states
- Danger buttons and destructive actions

#### Racing (Green - Success)
```css
--color-racing-50: #e8f7e8
--color-racing-100: #d3e5d3
--color-racing-200: #b8cdb8
--color-racing-300: #a0b8a0
--color-racing-400: #7b967b
--color-racing-500: #617a61   /* Success states */
--color-racing-600: #4e664e
--color-racing-700: #415441
--color-racing-800: #344234
--color-racing-900: #2d382d
```

**Usage:**
- `racing-500`: Success messages, running states
- Success tags and badges

#### Apricot (Orange - Warning)
```css
--color-apricot-50: #faf3e3
--color-apricot-100: #fceccc
--color-apricot-200: #fcdca3
--color-apricot-300: #f7c77e   /* Warning light */
--color-apricot-400: #eba54b
--color-apricot-500: #e39740   /* Warning states */
--color-apricot-600: #d18538
--color-apricot-700: #995b25
--color-apricot-800: #85491c
--color-apricot-900: #6b3b1b
```

**Usage:**
- `apricot-500`: Warning messages, pending states
- BETA tags, caution indicators

#### Geode (Purple)
```css
--color-geode-50: #f4f0f5
--color-geode-100: #ebe4ed
--color-geode-200: #e7dbea
--color-geode-300: #d9c5dc
--color-geode-400: #c8adcc
--color-geode-500: #b997bd
--color-geode-600: #a37ea6
--color-geode-700: #7c5d7d
--color-geode-800: #7c5d7d
--color-geode-900: #523f52
```

#### Fluoro (Bright Green/Yellow)
```css
--color-fluoro-50: #f7ffcc
--color-fluoro-100: #f0fcac
--color-fluoro-200: #e8fa82
--color-fluoro-300: #dff75e
--color-fluoro-400: #d3f52c
--color-fluoro-500: #c6e51c   /* Accent/highlight */
--color-fluoro-600: #b7cc3d
--color-fluoro-700: #88991c
--color-fluoro-800: #667302
--color-fluoro-900: #53591e
```

#### Base (Neutral Grays)
```css
--color-base-0: #ffffff      /* Pure white */
--color-base-50: #fcfcf8     /* Off-white, main backgrounds */
--color-base-100: #e6e6e2    /* Light gray */
--color-base-200: #d0d0cd    /* Borders */
--color-base-300: #babab7
--color-base-400: #a4a4a2
--color-base-500: #8e8e8c
--color-base-600: #737371
--color-base-700: #626261
--color-base-800: #4c4c4c    /* Secondary text */
--color-base-900: #363636    /* Primary text */
--color-base-950: #202020    /* Darkest, primary buttons */
```

### Semantic Colors (Light Mode)

```css
/* Backgrounds */
--color-fill-container: #ffffff          /* Card backgrounds */
--color-fill-secondary: #f7f5f2          /* Hover states */
--color-fill-tertiary: #f2eeeb           /* Selected states */
--color-fill-quaternary: rgba(0, 0, 0, 0.02)

/* Borders */
--color-border: #cfd6df                  /* Primary borders */
--color-border-secondary: #d0d0cd        /* Subtle borders */

/* Text */
--color-text: #3f434c                    /* Primary text */
--color-text-secondary: rgba(0, 0, 0, 0.45)  /* Secondary text */
--color-text-tertiary: #858c9d           /* Tertiary text */

/* Brand */
--color-primary: #202020                 /* Primary actions */
--color-success: #32b77f
--color-error: #e43920
--color-warning: #e8b500
```

### Semantic Colors (Dark Mode)

```css
[data-theme='dark'] {
  --color-fill-container: #141414
  --color-fill-secondary: #1f1f1f
  --color-fill-tertiary: #1a1a1a
  --color-fill-quaternary: rgba(255, 255, 255, 0.04)

  --color-border: #434343
  --color-border-secondary: #3a3a3a

  --color-text: rgba(255, 255, 255, 0.85)
  --color-text-secondary: rgba(255, 255, 255, 0.65)
  --color-text-tertiary: rgba(255, 255, 255, 0.45)

  --color-primary: #177ddc
  --color-success: #49aa19
  --color-error: #d32029
  --color-warning: #d89614
}
```

### Using Colors in Components

#### With Tailwind Classes
```tsx
<div className="bg-[var(--color-stone-50)] text-[var(--color-text)]">
<div className="border-[var(--color-border-secondary)]">
```

#### With Inline Styles
```tsx
<div style={{
  background: 'var(--color-fill-secondary)',
  borderColor: 'var(--color-border)'
}}>
```

#### With SCSS Variables
```scss
background: var(--color-stone-50);
color: var(--iom-stone-text);
border: 1px solid var(--color-border-secondary);
```

---

## Typography

### Font Family
```css
:root {
  font-family: Inter, system-ui, Avenir, Helvetica, Arial, sans-serif;
}
```

### Type Scale

#### Headings
| Size | CSS Variable | Pixels | Usage |
|------|--------------|--------|-------|
| H1 | `--text-heading-1` | 40px | Page titles |
| H2 | `--text-heading-2` | 36px | Section headers |
| H3 | `--text-heading-3` | 32px | Subsection headers |
| H4 | `--text-heading-4` | 28px | Card titles |
| H5 | `--text-heading-5` | 24px | Small headings |
| H6 | `--text-heading-6` | 20px | List headers |

#### Body Text
| Size | CSS Variable | Pixels | Usage |
|------|--------------|--------|-------|
| Large | `--text-body-lg` | 18px | Emphasized content |
| Medium | `--text-body-md` | 16px | Default body text |
| Small | `--text-body-sm` | 14px | Labels, captions |
| X-Small | `--text-body-xs` | 12px | Table headers, metadata |

### Font Weights

```css
font-weight: 300;  /* Light (rarely used) */
font-weight: 400;  /* Normal (default) */
font-weight: 500;  /* Medium (emphasis, buttons) */
font-weight: 700;  /* Bold (strong emphasis) */
```

**Usage Guidelines:**
- **400**: Default for all body text
- **500**: Form labels, emphasized text, button labels, table headers
- **700**: Very strong emphasis only (use sparingly)
- **Note**: `<strong>` tags are overridden to 500, not 700

### Text Component Pattern

```tsx
import { Text } from '@/components/UI/Text';

<Text
  size="bodyMedium"      // heading1-6, bodyLarge, bodyMedium, bodySmall, bodyXSmall
  weight="normal"         // light, normal, medium, bold
  variant="primary"       // primary, secondary, tertiary, danger
  italic={false}
>
  Content
</Text>
```

**Maps to:**
- `primary`: `var(--color-text)`
- `secondary`: `var(--color-text-secondary)`
- `tertiary`: `var(--color-text-tertiary)`
- `danger`: `var(--color-error)`

### Ant Design Typography

```tsx
import { Typography } from 'antd';

// Headings
<Typography.Title level={1} style={{ margin: 0 }}>
<Typography.Title level={4} style={{ margin: 0, fontWeight: 'normal' }}>

// Text
<Typography.Text>Default text</Typography.Text>
<Typography.Text type="secondary">Secondary text</Typography.Text>
<Typography.Text type="danger">Error text</Typography.Text>

// With custom weight
<Typography.Text style={{ fontWeight: 500 }}>Medium weight</Typography.Text>
```

### Tailwind Typography Classes

```tsx
// Size
className="text-xs"      // 0.75rem (12px)
className="text-sm"      // 0.875rem (14px)
className="text-base"    // 1rem (16px)
className="text-lg"      // 1.125rem (18px)
className="text-[11px]"  // Custom size

// Weight
className="font-normal"  // 400
className="font-medium"  // 500
className="font-bold"    // 700

// Color
className="text-[var(--color-text)]"
className="text-[var(--color-text-secondary)]"

// Utilities
className="whitespace-nowrap"
className="text-ellipsis overflow-hidden"
className="line-through"
```

### Line Height

Default line heights are handled by Ant Design theme:
- Headings: 1.2 - 1.4
- Body text: 1.5 - 1.7
- Compact text (tables): 1.3 - 1.5

---

## Spacing System

### Base Spacing Scale (Tailwind)

| Class | Value | Pixels |
|-------|-------|--------|
| `0` | 0 | 0px |
| `0.5` | 0.125rem | 2px |
| `1` | 0.25rem | 4px |
| `1.5` | 0.375rem | 6px |
| `2` | 0.5rem | 8px |
| `2.5` | 0.625rem | 10px |
| `3` | 0.75rem | 12px |
| `4` | 1rem | 16px |
| `5` | 1.25rem | 20px |
| `6` | 1.5rem | 24px |
| `8` | 2rem | 32px |
| `10` | 2.5rem | 40px |
| `12` | 3rem | 48px |

### Ant Design Gap Scale

```tsx
<Flex gap="small">   // 8px
<Flex gap="middle">  // 16px
<Flex gap="large">   // 24px
<Flex gap={32}>      // Custom (32px)
```

### Spacing Hierarchy

#### Component-Level Spacing
```tsx
// Icon-text pairs
<Flex gap={6} align="center">
  <Icon /> <span>Text</span>
</Flex>

// Button groups
<Flex gap="small">  // 8px
  <Button>Action</Button>
  <Button>Action</Button>
</Flex>

// Form fields
<Flex vertical gap={12}>
  <FormItem />
  <FormItem />
</Flex>

// Section items
<Flex vertical gap={16}>
  <Card />
  <Card />
</Flex>
```

#### Page-Level Spacing
```tsx
// Major sections
<Flex gap={32} vertical>
  <Section />
  <Section />
</Flex>

// Content sections with titles
<Flex gap={24} vertical>
  <Title />
  <Content />
</Flex>
```

### Padding Patterns

#### Containers
```scss
// Default container
padding: 1rem;                    // Mobile
padding: 1.4rem;                  // Desktop horizontal

// Card body
padding: 16px;                    // size="default"
padding: 12px;                    // size="small"
padding: 24px;                    // size="large"
```

#### Common Padding Classes
```tsx
className="p-1"         // 4px all sides
className="p-1.5"       // 6px all sides
className="px-1.5 py-0.5"  // Tag padding
className="px-4 py-2"   // Button-like padding
```

### Margin Patterns

**Philosophy**: Prefer flexbox gap over margins for spacing between siblings.

```tsx
// ✅ Good: Using gap
<Flex gap={16}>
  <Card />
  <Card />
</Flex>

// ❌ Avoid: Using margins
<div>
  <Card className="mb-4" />
  <Card />
</div>
```

**When to use margins:**
- Overriding default margins (e.g., `style={{ margin: 0 }}`)
- Centering with `mx-auto`
- Single-direction spacing that doesn't fit gap pattern

---

## Component Styles

### Layout Components

#### Page Component

```tsx
import { Page } from '@/components/UI/Page';

<Page
  actions={<Button>Action</Button>}
  breadcrumbItems={[{ path: '/', title: 'Home' }]}
  subTitle="Page subtitle"
  metaTitle="Browser Tab Title"
  container={true}
  containerSize="lg"      // sm, md, lg, xl
  fullHeight={true}
  skeleton={isLoading}
>
  <Content />
</Page>
```

**Structure:**
```
┌─ Breadcrumb ──────────────────────┐
│ Home / Section / Page             │
├───────────────────────────────────┤
│ Title                    [Actions]│
│ Subtitle                          │
├───────────────────────────────────┤
│                                   │
│ Content (with container)          │
│                                   │
└───────────────────────────────────┘
```

#### Container Component

```tsx
import { Container } from '@/components/UI/Container';

<Container size="lg">  // sm (680px), md (768px), lg (1024px), xl (1280px)
  <Content />
</Container>
```

**Styling:**
- Default: 100% width, 1rem padding, 1.4rem horizontal padding
- Auto left/right margins for centering
- `max-width` based on size prop

#### PageLayout Component (Two-Column)

```tsx
import { PageLayout } from '@/components/UI/PageLayout';

<PageLayout
  sidebarMenu={
    <Menu
      items={[...]}
      selectedKeys={[activeKey]}
    />
  }
>
  <Content />
</PageLayout>
```

**Layout:**
```
┌────────┬───────────────────────────┐
│        │                           │
│ Sidebar│ Content                   │
│ 224px  │ flex-1                    │
│        │                           │
│ sticky │                           │
└────────┴───────────────────────────┘
```

**Sidebar Styling:**
- Width: `224px`
- Border: `1px solid var(--color-border-secondary)`
- Position: `sticky` with `top: 0`

### Button Components

#### IomButton (Icon Button)

```tsx
import { IomButton } from '@/components/UI/IomButton';

<IomButton
  icon={<DotsThreeVertical size={16} />}
  onClick={() => {}}
  disabled={false}
/>
```

**Styling:**
```scss
.IomButton {
  background: none;
  border: none;
  width: 32px;
  height: 32px;
  padding: 4px;
  border-radius: 2px;
  color: var(--color-text-secondary);

  &:hover {
    background: var(--color-fill-secondary);
    color: var(--color-text);
  }

  &:active {
    background: var(--color-fill);
  }
}
```

#### Ant Design Button Patterns

```tsx
// Primary action (default filled)
<Button type="primary">Primary</Button>

// Secondary action (outlined)
<Button type="default">Secondary</Button>

// Tertiary action (text only)
<Button type="text">Tertiary</Button>

// Danger action
<Button danger type="primary">Delete</Button>

// Sizes
<Button size="small">Small</Button>      // height: 24px
<Button size="middle">Middle</Button>     // height: 32px (default)
<Button size="large">Large</Button>       // height: 40px

// Block (full width)
<Button block>Full Width</Button>
```

**Custom Primary Button Style:**
```tsx
<Button
  type="default"
  className="iom-primary-btn"
>
  Custom Primary
</Button>
```

Renders with hover state colors by default.

### Card Components

#### Ant Design Card

```tsx
import { Card } from 'antd';

<Card
  title="Card Title"
  extra={<Button type="text">Action</Button>}
  size="small"              // small, default, large
  bordered={true}
  styles={{
    body: { padding: 16 }
  }}
>
  <Content />
</Card>
```

**Standard Card Styling:**
```tsx
<Card
  style={{ width: 360 }}
  styles={{ body: { minHeight: 120 } }}
  size="small"
>
  <Flex gap="middle" vertical>
    <Title />
    <Description />
  </Flex>
</Card>
```

#### EmptyCard Pattern

```tsx
import { EmptyCard } from '@/components/UI/EmptyCard';

<EmptyCard bordered={true}>
  <Empty
    description="No data"
    image={Empty.PRESENTED_IMAGE_SIMPLE}
  />
</EmptyCard>
```

**Styling:**
```scss
.emptyCard {
  padding: 1rem;
}

.bordered {
  border: 1px solid var(--color-border-secondary);
}
```

#### FormRadioCard (Interactive Card)

```tsx
<FormRadioCard
  options={[
    {
      value: 'option1',
      label: 'Option Label',
      icon: <Icon />,
      description: 'Detailed description of option',
      disabled: false,
      tooltip: 'Additional tooltip info'
    }
  ]}
  value={selectedValue}
  onChange={handleChange}
  span={12}           // Grid column span (24-column grid)
  size="small"
/>
```

**Hover Styling:**
```scss
.FormRadioCard {
  cursor: pointer;
  border: 1px solid var(--color-border);

  &:not([disabled]):hover {
    border-color: var(--ant-input-hover-border-color);
  }
}
```

### Table Components

#### DataTableV2 (Recommended)

```tsx
import { DataTableV2 } from '@/components/UI/DataTable';

<DataTableV2
  tableName="unique-table-name"
  columns={columns}
  dataSource={data}
  isLoading={isFetching}
  onRefresh={() => refetch()}
  searchKeys={['name', 'description']}
  pagination={{
    defaultPageSize: 20,
    showSizeChanger: true,
    showTotal: (total) => `Total ${total} items`
  }}
  rowKey="id"
  size="middle"          // small, middle, large
/>
```

**Table Styling Overrides:**
```scss
// Header padding for large size
.ant-table:not(.ant-table-middle):not(.ant-table-small) .ant-table-thead th {
  padding: 12px 16px !important;
}

// Header font size
th.ant-table-cell {
  font-size: 12px;
}

// Remove column separator lines
.ant-table-thead > tr > th::before {
  display: none;
}
```

**Column Definition Pattern:**
```tsx
const columns = [
  {
    title: 'Name',
    dataIndex: 'name',
    key: 'name',
    width: 200,
    render: (text) => <Typography.Text>{text}</Typography.Text>
  },
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    width: 100,
    render: (status) => <ViewStatus status={status} />
  },
  {
    title: 'Actions',
    key: 'actions',
    width: 80,
    align: 'center',
    render: (_, record) => (
      <ActionsMenu
        menu={{ items: [...] }}
        buttonProps={{ type: 'text', size: 'small' }}
      />
    )
  }
];
```

#### GenericDetails (Key-Value Table)

```tsx
import { GenericDetails } from '@/components/UI/GenericDetails';

<GenericDetails
  details={[
    { label: 'Name', value: 'John Doe' },
    { label: 'Email', value: 'john@example.com' },
    { label: 'Status', value: <Tag color="stone">Active</Tag> }
  ]}
  size="small"
  bordered={true}
/>
```

**Renders as:**
```
┌─────────────┬──────────────────┐
│ Name        │ John Doe         │
├─────────────┼──────────────────┤
│ Email       │ john@example.com │
├─────────────┼──────────────────┤
│ Status      │ [Active]         │
└─────────────┴──────────────────┘
```

**Styling:**
- Label column: `240px` width
- `size="small"` with `bordered={true}`
- `showHeader={false}`
- Custom border radius on corners

### Tag Components

#### Custom Tag Component

```tsx
import { Tag } from '@/components/UI/Tag';

<Tag
  color="stone"       // stone, lake, racing, reef, apricot, geode
  style="medium"      // light, medium, dark, stroke
>
  Label
</Tag>
```

**Color-Style Matrix:**

| Color | Light | Medium | Dark | Stroke |
|-------|-------|--------|------|--------|
| stone | `stone-100-surface` | `stone-300-surface` | `stone-900-surface` | `stone-50-surface + border` |
| lake | `lake-300` bg | `lake-500` bg | `lake-900` bg | `lake` border |
| racing | Light green | Medium green | Dark green | Green border |
| reef | Light coral | Medium coral | Dark coral | Coral border |
| apricot | Light orange | Medium orange | Dark orange | Orange border |

**Styling Pattern:**
```scss
.tag-light {
  background: var(--iom-stone-100-surface);
  color: var(--iom-stone-100-text);
}

.tag-medium {
  background: var(--iom-stone-300-surface);
  color: var(--iom-stone-text);
}

.tag-stroke {
  background: var(--iom-stone-50-surface);
  border: 1px solid var(--iom-stone-50-border);
  color: var(--iom-stone-50-text);
}
```

#### Ant Design Tag

```tsx
import { Tag } from 'antd';

// Status tags
<Tag color="success">Running</Tag>
<Tag color="error">Failed</Tag>
<Tag color="warning">Pending</Tag>
<Tag color="default">Stopped</Tag>

// Custom color
<Tag color="#f50">Custom</Tag>

// With close
<Tag closable onClose={() => {}}>Closable</Tag>
```

### Status Components

#### ViewStatus

```tsx
import { ViewStatus } from '@/components/UI/Views';

<ViewStatus
  status="RUNNING"      // RUNNING, FAILED, PENDING, COMPLETED, etc.
  showLabel={true}
/>
```

**Status Color Mapping:**
```tsx
RUNNING: { icon: CircleIcon, color: 'var(--color-success)' }
FAILED: { icon: WarningCircleIcon, color: 'var(--color-error)' }
PENDING: { icon: CircleDashedIcon, color: 'var(--color-warning)' }
COMPLETED: { icon: CheckCircleIcon, color: 'var(--color-text-description)' }
```

### Form Components

#### Standard Form Item

```tsx
import { Form, Input } from 'antd';

<Form.Item
  label="Field Label"
  name="fieldName"
  rules={[{ required: true, message: 'Required field' }]}
  labelCol={{ span: 24 }}  // Full width label
>
  <Input placeholder="Enter value" />
</Form.Item>
```

**Form Item Styling:**
```scss
.ant-form-item-label {
  padding-bottom: 0 !important;

  label {
    height: 28px !important;
    font-weight: 400;  // Not bold by default
  }

  .ant-form-item-optional {
    font-weight: 400;
  }
}
```

#### FormRadioCard

```tsx
import { FormRadioCard } from '@/components/UI/Forms/FormRadioCard';

<Form.Item name="deployment" label="Deployment Type">
  <FormRadioCard
    options={[
      {
        value: 'kubernetes',
        label: 'Kubernetes',
        icon: <Icon />,
        description: 'Deploy on Kubernetes cluster',
        tooltip: 'Requires K8s setup'
      },
      {
        value: 'standalone',
        label: 'Standalone',
        icon: <Icon />,
        description: 'Single instance deployment'
      }
    ]}
    span={12}  // 2 columns
  />
</Form.Item>
```

#### FormControlKeyValue

```tsx
import { FormControlKeyValue } from '@/components/UI/Forms';

<Form.Item name="labels" label="Labels">
  <FormControlKeyValue
    keyPlaceholder="Key"
    valuePlaceholder="Value"
    addButtonText="Add Label"
  />
</Form.Item>
```

### Menu & Dropdown Components

#### ActionsMenu (3-Dot Menu)

```tsx
import { ActionsMenu } from '@/components/UI/ActionsMenu';

<ActionsMenu
  menu={{
    items: [
      {
        key: 'edit',
        label: 'Edit',
        icon: <PencilIcon />,
        onClick: () => handleEdit()
      },
      {
        type: 'divider'
      },
      {
        key: 'delete',
        label: 'Delete',
        icon: <TrashIcon />,
        danger: true,
        onClick: () => handleDelete()
      }
    ]
  }}
  buttonProps={{
    type: 'text',
    size: 'small',
    icon: <DotsThreeVertical />
  }}
/>
```

**Dropdown Styling:**
```scss
.ant-dropdown,
.ant-popover {
  border: 1px solid var(--color-border-secondary);
  border-radius: var(--ant-border-radius);
}

.iom-list-menu-dropdown ul li {
  padding: 0 !important;

  button {
    justify-content: start !important;
    padding-left: 8px !important;
    background-color: unset !important;
  }
}
```

### Modal & Confirmation Components

#### DeleteConfirm

```tsx
import { DeleteConfirm } from '@/components/UI/DeleteConfirm';

<DeleteConfirm
  title="Delete Resource?"
  description="This will permanently delete the resource. This action cannot be undone."
  buttonName="Delete"
  onConfirm={handleDelete}
  buttonProps={{
    type: 'text',
    danger: true,
    size: 'small'
  }}
/>
```

**Modal Configuration:**
```tsx
{
  okButtonProps: {
    danger: true,
    size: 'middle'
  },
  cancelButtonProps: {
    variant: 'outlined',
    size: 'middle'
  },
  centered: true,
  maskClosable: true,
  closable: true,
  icon: null
}
```

#### Ant Design Modal

```tsx
import { Modal } from 'antd';

<Modal
  title="Modal Title"
  open={isOpen}
  onOk={handleOk}
  onCancel={handleCancel}
  centered={true}
  okText="Confirm"
  cancelText="Cancel"
  okButtonProps={{ size: 'middle' }}
  cancelButtonProps={{ size: 'middle' }}
>
  <Content />
</Modal>
```

---

## Shadows & Elevation

### Ant Design Shadow Variables

```scss
// Standard shadow (default for dropdowns, popovers)
--ant-box-shadow: 0 6px 16px 0 rgba(0, 0, 0, 0.08),
                  0 3px 6px -4px rgba(0, 0, 0, 0.12),
                  0 9px 28px 8px rgba(0, 0, 0, 0.05)

// Secondary shadow (subtle elevation)
--ant-box-shadow-secondary: 0 6px 16px 0 rgba(0, 0, 0, 0.08),
                            0 3px 6px -4px rgba(0, 0, 0, 0.12)
```

### Usage Patterns

```tsx
// Inline style
style={{ boxShadow: 'var(--ant-box-shadow-secondary)' }}

// Tailwind class
className="shadow-[var(--ant-box-shadow)]"

// Custom shadow
className="shadow-lg"  // Tailwind's large shadow
className="shadow-[-1px_1px_3px_0px_rgba(0,0,0,0.2)]"
```

### Elevation Hierarchy

| Level | Shadow | Usage |
|-------|--------|-------|
| 0 | None | Flat surfaces, inline elements |
| 1 | `shadow-sm` | Cards at rest |
| 2 | `var(--ant-box-shadow-secondary)` | Elevated cards, hover states |
| 3 | `var(--ant-box-shadow)` | Dropdowns, popovers, modals |
| 4 | `shadow-lg` | Floating elements, tooltips |

### Component-Specific Shadows

```tsx
// Dropdown menus
.ant-dropdown {
  box-shadow: var(--ant-box-shadow);
}

// Popovers
.ant-popover {
  box-shadow: var(--ant-box-shadow);
}

// Cards on hover
.card-hover:hover {
  box-shadow: var(--ant-box-shadow-secondary);
}

// Toolbar (custom)
.toolbar {
  box-shadow: var(--ant-box-shadow);
}
```

---

## Animations & Transitions

### Standard Transitions

```scss
// Background color transitions (hover states)
transition: background-color 0.3s;
transition: background-color 0.3s ease;

// Border color transitions
transition: border-color 0.3s;

// Opacity transitions
transition: opacity 0.3s linear;

// Combined transitions
transition: background-color 0.3s, border-color 0.3s;
transition: opacity 0.2s linear, visibility 0.3s linear;
```

### Tailwind Transition Classes

```tsx
// Color transitions
className="transition-colors duration-300"

// All properties
className="transition-all duration-200"

// Opacity
className="transition-opacity duration-150"

// Transform
className="transition-transform duration-200 hover:scale-105"
```

### Component Animations

#### Hover Visible Pattern

```scss
// Copy icon that appears on hover
.iom-hover-visible.ant-typography {
  .ant-typography-copy {
    transition: visibility 0.3s linear, opacity 0.2s linear;
    opacity: 0;
    visibility: hidden;
  }

  &:hover .ant-typography-copy {
    opacity: 1;
    visibility: visible;
  }
}
```

Usage:
```tsx
<Typography.Text className="iom-hover-visible" copyable>
  Hover to show copy icon
</Typography.Text>
```

#### Button Hover States

```scss
.IomButton {
  transition: background-color 0.2s, color 0.2s;

  &:hover {
    background: var(--color-fill-secondary);
    color: var(--color-text);
  }

  &:active {
    background: var(--color-fill);
  }
}
```

#### Card Hover

```scss
.card-hover {
  transition: background-color 0.3s;

  &:hover {
    background-color: var(--color-fill-tertiary);
    cursor: pointer;
  }
}
```

### Keyframe Animations

#### Fullscreen Animation

```scss
@keyframes enterFullscreen {
  from {
    opacity: 0;
    transform: scale(0.95);
  }
  to {
    opacity: 1;
    transform: scale(1);
  }
}

.fullscreen-enter {
  animation: enterFullscreen 0.3s ease-in-out forwards;
}
```

#### Spin Animation (Loading)

```tsx
import { Spin } from 'antd';

<Spin size="small" />
<Spin size="default" />
<Spin size="large" />

// Custom indicator
<Spin indicator={<LoadingOutlined spin />} />
```

### Animation Guidelines

1. **Duration**: 150-300ms for most interactions
2. **Easing**:
   - `ease`: Default for most animations
   - `ease-in-out`: Smooth start and end
   - `linear`: Opacity/visibility changes
3. **Performance**: Prefer `transform` and `opacity` over `width`, `height`, `top`, `left`
4. **Accessibility**: Respect `prefers-reduced-motion`

```css
@media (prefers-reduced-motion: reduce) {
  * {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
```

---

## Border Radius

### Border Radius Scale

```scss
// Ant Design variables
--ant-border-radius: 6px           // Default for most components
--ant-border-radius-sm: 4px        // Small components
--ant-border-radius-lg: 8px        // Large components
--ant-table-header-border-radius: 8px
```

### Component Border Radius

| Component | Radius | Value |
|-----------|--------|-------|
| IomButton | Fixed | `2px` |
| Cards | Default | `var(--ant-border-radius)` = `6px` |
| Tags | Small | `4px` |
| Inputs | Default | `var(--ant-border-radius)` = `6px` |
| Buttons | Default | `var(--ant-border-radius)` = `6px` |
| Table | Large | `var(--ant-table-header-border-radius)` = `8px` |
| Modals | Large | `var(--ant-border-radius-lg)` = `8px` |
| Dropdowns | Default | `var(--ant-border-radius)` = `6px` |

### Tailwind Border Radius Classes

```tsx
className="rounded"       // 0.25rem (4px) - Tailwind default
className="rounded-sm"    // 0.125rem (2px)
className="rounded-md"    // 0.375rem (6px)
className="rounded-lg"    // 0.5rem (8px)
className="rounded-full"  // 9999px (circle)

// Individual corners
className="rounded-t"     // Top corners
className="rounded-b"     // Bottom corners
className="rounded-l"     // Left corners
className="rounded-r"     // Right corners
```

### Border Radius Patterns

```scss
// Standard card
border-radius: var(--ant-border-radius);  // 6px

// Small elements (tags, badges)
border-radius: 4px;

// Icon buttons
border-radius: 2px;

// Pills/badges
border-radius: 999px;

// Tables with rounded corners
.iom-ant-table-border-bottom tr:last-child {
  td:first-child {
    border-bottom-left-radius: calc(var(--ant-table-header-border-radius) - var(--ant-line-width));
  }

  td:last-child {
    border-bottom-right-radius: calc(var(--ant-table-header-border-radius) - var(--ant-line-width));
  }
}
```

---

## Opacity & Transparency

### Base Opacity Variants

```scss
/* Dark opacity variants (on light backgrounds) */
--color-base-950-0: #20202000    /* 0% opacity */
--color-base-950-20: #20202033   /* 20% opacity */
--color-base-950-40: #20202066   /* 40% opacity */
--color-base-950-60: #20202099   /* 60% opacity */
--color-base-950-80: #202020cc   /* 80% opacity */

/* Light opacity variants (on dark backgrounds) */
--color-base-0-0: #ffffff00      /* 0% opacity */
--color-base-0-20: #ffffff33     /* 20% opacity */
--color-base-0-40: #ffffff66     /* 40% opacity */
--color-base-0-60: #ffffff99     /* 60% opacity */
--color-base-0-80: #ffffffcc     /* 80% opacity */

/* Stone opacity variants */
--color-base-50-0: #fcfcf800
--color-base-50-20: #fcfcf833
--color-base-50-40: #fcfcf866
--color-base-50-60: #fcfcf899
--color-base-50-80: #fcfcf8cc
```

### Using Opacity

#### With CSS Variables
```tsx
<div style={{
  background: 'var(--color-base-950-20)',  // 20% black
  color: 'var(--color-base-0-80)'          // 80% white
}} />
```

#### With Tailwind
```tsx
className="bg-black/20"        // 20% opacity black
className="text-white/80"      // 80% opacity white
className="border-stone-300/50" // 50% opacity stone-300
```

#### With rgba()
```tsx
style={{
  background: 'rgba(0, 0, 0, 0.02)',      // Quaternary fill
  color: 'rgba(0, 0, 0, 0.45)'            // Secondary text
}}
```

### Opacity Scale

| Percentage | Hex Alpha | Usage |
|------------|-----------|-------|
| 0% | `00` | Invisible |
| 10% | `1a` | Very subtle backgrounds |
| 20% | `33` | Subtle overlays |
| 40% | `66` | Moderate overlays |
| 45% | `73` | Secondary text (light mode) |
| 60% | `99` | Strong overlays |
| 65% | `a6` | Secondary text (dark mode) |
| 80% | `cc` | Almost opaque |
| 85% | `d9` | Primary text (dark mode) |
| 100% | `ff` | Fully opaque |

### Component Opacity Patterns

#### Disabled States
```scss
.ant-btn[disabled] {
  opacity: 0.4;
  cursor: not-allowed;
}

.ant-input[disabled] {
  opacity: 1;  // Use background color instead
  background-color: var(--ant-color-bg-container-disabled);
}
```

#### Hover States
```scss
// Icon hover
.icon:hover {
  opacity: 0.8;
}

// Image hover
.image-hover:hover {
  opacity: 0.9;
}
```

#### Loading States
```scss
.loading-overlay {
  background: rgba(255, 255, 255, 0.8);
  backdrop-filter: blur(2px);
}
```

---

## Common Tailwind CSS Usage

### Most Frequently Used Classes

#### Layout & Flexbox
```tsx
// Flex container
className="flex"
className="inline-flex"
className="flex flex-col"

// Alignment
className="items-center"
className="items-start"
className="items-end"
className="justify-between"
className="justify-center"
className="justify-start"

// Gap
className="gap-1"      // 4px
className="gap-2"      // 8px
className="gap-3"      // 12px
className="gap-4"      // 16px

// Common combinations
className="flex items-center gap-2"
className="flex justify-between items-center"
className="inline-flex items-center gap-1"
```

#### Spacing
```tsx
// Padding
className="p-1"         // 4px all
className="p-1.5"       // 6px all
className="px-1.5 py-0.5"  // Horizontal 6px, vertical 2px
className="px-4 py-2"   // Button-like

// Margin
className="m-0"         // Reset margin
className="mx-auto"     // Center horizontally
className="mb-4"        // Bottom margin 16px
className="mt-6"        // Top margin 24px
```

#### Sizing
```tsx
// Width
className="w-full"      // 100%
className="w-fit"       // fit-content
className="w-[200px]"   // Custom 200px
className="min-w-0"     // Allow flex shrinking

// Height
className="h-full"      // 100%
className="h-[140px]"   // Custom 140px
className="min-h-0"     // Allow flex shrinking
```

#### Typography
```tsx
// Size
className="text-xs"     // 12px
className="text-sm"     // 14px
className="text-base"   // 16px
className="text-[11px]" // Custom

// Weight
className="font-normal"  // 400
className="font-medium"  // 500
className="font-bold"    // 700

// Color
className="text-[var(--color-text)]"
className="text-[var(--color-text-secondary)]"

// Utilities
className="whitespace-nowrap"
className="text-ellipsis overflow-hidden"
className="line-through"
```

#### Borders
```tsx
// Border width
className="border"                  // 1px all sides
className="border-t"                // Top only
className="border-b"                // Bottom only

// Border color
className="border-[var(--color-border)]"
className="border-[var(--color-border-secondary)]"

// Border radius
className="rounded"                 // 4px
className="rounded-sm"              // 2px
className="rounded-lg"              // 8px
```

#### Backgrounds
```tsx
className="bg-white"
className="bg-[var(--color-fill-secondary)]"
className="bg-[var(--color-stone-50)]"
className="bg-black/20"             // 20% opacity
```

#### Visibility & Display
```tsx
className="hidden"
className="block"
className="inline-block"
className="overflow-hidden"
className="overflow-auto"
```

#### Position
```tsx
className="relative"
className="absolute"
className="sticky"
className="fixed"
```

### Custom Utility Classes

**File:** `src/styles/index.css`

```css
.whitespace-nowrap { white-space: nowrap; }
.whitespace-normal { white-space: normal; }
.overflow-hidden { overflow: hidden; }
.justify-start { justify-content: flex-start; }
.hover-underline:hover { text-decoration: underline !important; }
.no-select { user-select: none; }
.no-padding { padding: 0 !important; }
.no-margin { margin: 0 !important; }
.display-none { display: none !important; }
.ellipsis {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
}
.cursor-pointer { cursor: pointer; }
.line-through { text-decoration: line-through; }
```

**File:** `src/styles/App.scss`

```css
.iom-content-800 { max-width: 800px; }
.iom-content-md { max-width: 768px; }
.iom-content-lg { max-width: 992px; }
.iom-content-xl { max-width: 1200px; }

.iom-uppercase-title { text-transform: uppercase; }

.card-hover:hover {
  background-color: var(--color-fill-tertiary);
  border-radius: 4px;
  cursor: pointer;
}

.weight-bold { font-weight: 500 !important; }

.tab-count-tag {
  font-size: 10px;
  background: var(--color-fill-tertiary);
  border-radius: 4px;
  padding: 0 6px;
  font-weight: 400;
  color: var(--color-text-secondary);
}
```

---

## Example Component Reference

### Complete Page Layout Example

```tsx
import { Page } from '@/components/UI/Page';
import { Container } from '@/components/UI/Container';
import { DataTableV2 } from '@/components/UI/DataTable';
import { Button, Card, Flex, Typography } from 'antd';
import { Plus } from '@phosphor-icons/react';

function DomainsPage() {
  const { data, isLoading, refetch } = useDomainsQuery();

  return (
    <Page
      breadcrumbItems={[
        { path: '/admin', title: 'Admin' },
        { title: 'Domains' }
      ]}
      actions={
        <Button
          type="primary"
          icon={<Plus size={16} />}
          onClick={() => navigate('/admin/domains/new')}
        >
          Add Domain
        </Button>
      }
      container={true}
      containerSize="xl"
    >
      <Flex gap={32} vertical>
        {/* Header Section */}
        <Flex vertical gap={12}>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Domains
          </Typography.Title>
          <Typography.Text type="secondary">
            Manage your organization's data domains and access controls
          </Typography.Text>
        </Flex>

        {/* Stats Cards */}
        <Flex gap={16}>
          <Card size="small" style={{ flex: 1 }}>
            <Flex vertical gap={8}>
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                Total Domains
              </Typography.Text>
              <Typography.Title level={2} style={{ margin: 0 }}>
                {data?.length || 0}
              </Typography.Title>
            </Flex>
          </Card>

          <Card size="small" style={{ flex: 1 }}>
            <Flex vertical gap={8}>
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                Active Users
              </Typography.Text>
              <Typography.Title level={2} style={{ margin: 0 }}>
                {data?.reduce((sum, d) => sum + d.userCount, 0) || 0}
              </Typography.Title>
            </Flex>
          </Card>
        </Flex>

        {/* Data Table */}
        <DataTableV2
          tableName="domains-list"
          columns={columns}
          dataSource={data}
          isLoading={isLoading}
          onRefresh={refetch}
          searchKeys={['name', 'description']}
          pagination={{ defaultPageSize: 20 }}
          rowKey="id"
        />
      </Flex>
    </Page>
  );
}
```

### Card with Actions Example

```tsx
import { Card, Flex, Typography, Button } from 'antd';
import { Tag } from '@/components/UI/Tag';
import { ActionsMenu } from '@/components/UI/ActionsMenu';
import { PencilSimple, Trash, Cube } from '@phosphor-icons/react';

function ResourceCard({ resource, onEdit, onDelete }) {
  return (
    <Card
      size="small"
      styles={{ body: { padding: 16 } }}
      extra={
        <ActionsMenu
          menu={{
            items: [
              {
                key: 'edit',
                label: 'Edit',
                icon: <PencilSimple size={16} />,
                onClick: onEdit
              },
              {
                type: 'divider'
              },
              {
                key: 'delete',
                label: 'Delete',
                icon: <Trash size={16} />,
                danger: true,
                onClick: onDelete
              }
            ]
          }}
          buttonProps={{ type: 'text', size: 'small' }}
        />
      }
    >
      <Flex vertical gap={16}>
        {/* Header */}
        <Flex gap={12} align="start">
          <div className="flex items-center justify-center w-10 h-10 rounded bg-[var(--color-fill-secondary)]">
            <Cube size={20} />
          </div>

          <Flex vertical gap={4} style={{ flex: 1 }}>
            <Typography.Title level={5} style={{ margin: 0 }}>
              {resource.name}
            </Typography.Title>
            <Typography.Text type="secondary" className="text-sm">
              {resource.description}
            </Typography.Text>
          </Flex>
        </Flex>

        {/* Tags */}
        <Flex gap={8} wrap="wrap">
          <Tag color="stone" style="light">
            {resource.type}
          </Tag>
          <Tag color="lake" style="light">
            {resource.region}
          </Tag>
        </Flex>

        {/* Footer */}
        <Flex justify="between" align="center">
          <Typography.Text type="secondary" className="text-xs">
            Updated {formatRelativeTime(resource.updatedAt)}
          </Typography.Text>

          <Button type="text" size="small" onClick={() => navigate(resource.id)}>
            View Details
          </Button>
        </Flex>
      </Flex>
    </Card>
  );
}
```

### Form with RadioCards Example

```tsx
import { Form, Input, Button, Flex } from 'antd';
import { FormRadioCard } from '@/components/UI/Forms/FormRadioCard';
import { Database, CloudArrowUp } from '@phosphor-icons/react';

function DeploymentForm({ onSubmit }) {
  const [form] = Form.useForm();

  return (
    <Form
      form={form}
      layout="vertical"
      onFinish={onSubmit}
    >
      <Flex vertical gap={24}>
        {/* Deployment Type */}
        <Form.Item
          name="deploymentType"
          label="Deployment Type"
          rules={[{ required: true }]}
        >
          <FormRadioCard
            options={[
              {
                value: 'cloud',
                label: 'Cloud',
                icon: <CloudArrowUp size={24} />,
                description: 'Deploy to managed cloud infrastructure',
                tooltip: 'Recommended for production workloads'
              },
              {
                value: 'self-hosted',
                label: 'Self-Hosted',
                icon: <Database size={24} />,
                description: 'Deploy on your own infrastructure',
                tooltip: 'Full control over deployment'
              }
            ]}
            span={12}
          />
        </Form.Item>

        {/* Name Field */}
        <Form.Item
          name="name"
          label="Deployment Name"
          rules={[
            { required: true, message: 'Please enter a name' },
            { pattern: /^[a-z0-9-]+$/, message: 'Use lowercase letters, numbers, and hyphens' }
          ]}
        >
          <Input placeholder="my-deployment" />
        </Form.Item>

        {/* Actions */}
        <Flex gap="small" justify="end">
          <Button onClick={() => form.resetFields()}>
            Reset
          </Button>
          <Button type="primary" htmlType="submit">
            Create Deployment
          </Button>
        </Flex>
      </Flex>
    </Form>
  );
}
```

### Data Table with Custom Renderers Example

```tsx
import { DataTableV2 } from '@/components/UI/DataTable';
import { ViewStatus } from '@/components/UI/Views';
import { Tag } from '@/components/UI/Tag';
import { ActionsMenu } from '@/components/UI/ActionsMenu';
import { Typography, Tooltip } from 'antd';
import { formatRelativeTime } from '@/utils/date';

function JobsTable({ data, isLoading, onRefresh }) {
  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      render: (name, record) => (
        <Flex gap={8} align="center">
          <Typography.Link href={`/jobs/${record.id}`}>
            {name}
          </Typography.Link>
          {record.isPinned && (
            <Tag color="apricot" style="light">
              Pinned
            </Tag>
          )}
        </Flex>
      )
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 120,
      filters: [
        { text: 'Running', value: 'RUNNING' },
        { text: 'Failed', value: 'FAILED' },
        { text: 'Completed', value: 'COMPLETED' }
      ],
      onFilter: (value, record) => record.status === value,
      render: (status) => <ViewStatus status={status} showLabel />
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      key: 'duration',
      width: 100,
      render: (duration) => (
        <Typography.Text className="text-sm">
          {duration ? `${Math.round(duration / 1000)}s` : '-'}
        </Typography.Text>
      )
    },
    {
      title: 'Created',
      dataIndex: 'createdAt',
      key: 'createdAt',
      width: 150,
      sorter: (a, b) => new Date(a.createdAt) - new Date(b.createdAt),
      render: (date) => (
        <Tooltip title={new Date(date).toLocaleString()}>
          <Typography.Text type="secondary" className="text-xs">
            {formatRelativeTime(date)}
          </Typography.Text>
        </Tooltip>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      align: 'center',
      render: (_, record) => (
        <ActionsMenu
          menu={{
            items: [
              {
                key: 'view',
                label: 'View Details',
                onClick: () => navigate(`/jobs/${record.id}`)
              },
              {
                key: 'logs',
                label: 'View Logs',
                onClick: () => navigate(`/jobs/${record.id}/logs`)
              },
              { type: 'divider' },
              {
                key: 'restart',
                label: 'Restart',
                disabled: record.status !== 'FAILED',
                onClick: () => handleRestart(record.id)
              },
              {
                key: 'delete',
                label: 'Delete',
                danger: true,
                onClick: () => handleDelete(record.id)
              }
            ]
          }}
          buttonProps={{ type: 'text', size: 'small' }}
        />
      )
    }
  ];

  return (
    <DataTableV2
      tableName="jobs-table"
      columns={columns}
      dataSource={data}
      isLoading={isLoading}
      onRefresh={onRefresh}
      searchKeys={['name', 'id']}
      pagination={{
        defaultPageSize: 20,
        showSizeChanger: true,
        showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} jobs`
      }}
      rowKey="id"
    />
  );
}
```

### Custom Flex Component Example

```tsx
import { Flex } from '@/components/UI/Flex';
import { Typography } from 'antd';

function StatusBanner({ type, message }) {
  const colors = {
    info: 'var(--color-lake-300)',
    success: 'var(--color-racing-300)',
    warning: 'var(--color-apricot-300)',
    error: 'var(--color-reef-300)'
  };

  return (
    <Flex
      direction="row"
      justify="between"
      align="center"
      gap={4}
      style={{
        padding: '12px 16px',
        background: colors[type],
        borderRadius: '6px'
      }}
    >
      <Flex direction="row" align="center" gap={3}>
        <Icon type={type} />
        <Typography.Text style={{ fontWeight: 500 }}>
          {message}
        </Typography.Text>
      </Flex>

      <Button type="text" size="small">
        Dismiss
      </Button>
    </Flex>
  );
}
```

---

## Best Practices Summary

### Do's ✅

1. **Use Ant Design Flex with gap** instead of margin-based spacing
2. **Use CSS variables** for all colors to support theming
3. **Use semantic color names** (stone, lake, reef) over generic names
4. **Prefer Ant Design components** over custom implementations
5. **Use `type="text"` buttons** for tertiary actions
6. **Use IomButton** for icon-only buttons
7. **Use DataTableV2** for all data tables
8. **Use FormRadioCard** for selectable card options
9. **Maintain 500 font-weight** for emphasis (not 700)
10. **Test in both light and dark modes**

### Don'ts ❌

1. **Don't use margin** for sibling spacing (use gap instead)
2. **Don't hardcode colors** (use CSS variables)
3. **Don't use React.FC** type annotation
4. **Don't use `font-weight: 700`** for regular emphasis (use 500)
5. **Don't override Ant Design** unless absolutely necessary
6. **Don't use `!important`** unless overriding Ant Design styles
7. **Don't create custom components** when Ant Design provides one
8. **Don't use inline styles** when Tailwind classes work
9. **Don't mix px/rem inconsistently** (prefer rem via Tailwind)
10. **Don't forget to test responsiveness**

### Quick Reference Card

```tsx
// Page Layout
<Page container containerSize="lg">
  <Flex gap={32} vertical>
    <Section />
  </Flex>
</Page>

// Common Spacing
<Flex gap="small">        // 8px
<Flex gap="middle">       // 16px
<Flex gap={32}>           // 32px for major sections

// Common Colors
var(--color-stone-50)     // Sidebar background
var(--color-text)         // Primary text
var(--color-text-secondary) // Secondary text
var(--color-border)       // Primary borders

// Common Typography
className="text-xs"       // 12px
className="text-sm"       // 14px (most common)
className="text-base"     // 16px
className="font-medium"   // 500 weight

// Common Buttons
<Button type="primary">   // Primary action
<Button type="default">   // Secondary action
<Button type="text">      // Tertiary action
<IomButton icon={<Icon />} /> // Icon button

// Common Shadows
boxShadow: 'var(--ant-box-shadow-secondary)'
boxShadow: 'var(--ant-box-shadow)'
```

---

## File References

- **Tailwind Config**: `src/styles/tailwind-theme.css`
- **Color Palette**: `src/styles/iomete-colors.css`
- **Global Styles**: `src/styles/index.css`
- **Ant Overrides**: `src/styles/AntStyles.scss`
- **App Styles**: `src/styles/App.scss`
- **Theme Variables**: `src/styles/theme.css`
- **UI Components**: `src/components/UI/**`

---

**Last Updated**: 2025-11-01
**Version**: 1.0.0
**Maintainer**: IOMETE Development Team