import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router-dom";

import type { OpportunityTargetRow } from "../api/types";
import { formatPercent, formatPrice } from "../utils/format";

const columnHelper = createColumnHelper<OpportunityTargetRow>();

function percentCell(
  value: number | null,
  tone: "signed" | "favorable" | "adverse" | "neutral" = "neutral",
) {
  let className = "";
  if (value != null && tone === "signed") {
    className = value > 0 ? "positive-value" : value < 0 ? "negative-value" : "";
  } else if (value != null && tone === "favorable") {
    className = "positive-value";
  } else if (value != null && tone === "adverse") {
    className = "negative-value";
  }
  return <span className={className}>{formatPercent(value)}</span>;
}

const columns = [
  columnHelper.accessor("symbol", {
    header: "Symbol",
    cell: (info) => (
      <Link className="ticker-link" to={`/symbols/${encodeURIComponent(info.getValue())}`}>
        {info.getValue()}
        <ArrowUpRight size={14} />
      </Link>
    ),
  }),
  columnHelper.accessor("quality_status", { header: "Quality" }),
  columnHelper.accessor("open", { header: "O", cell: (info) => formatPrice(info.getValue()) }),
  columnHelper.accessor("high", { header: "H", cell: (info) => formatPrice(info.getValue()) }),
  columnHelper.accessor("low", { header: "L", cell: (info) => formatPrice(info.getValue()) }),
  columnHelper.accessor("close", { header: "C", cell: (info) => formatPrice(info.getValue()) }),
  columnHelper.accessor("previous_close", {
    header: "P",
    cell: (info) => formatPrice(info.getValue()),
  }),
  columnHelper.accessor("session_return", {
    header: "Return",
    cell: (info) => percentCell(info.getValue(), "signed"),
  }),
  columnHelper.accessor("gap", {
    header: "Gap",
    cell: (info) => percentCell(info.getValue(), "signed"),
  }),
  columnHelper.accessor("true_return", {
    header: "True Return",
    cell: (info) => percentCell(info.getValue(), "signed"),
  }),
  columnHelper.accessor("upside", {
    header: "Upside",
    cell: (info) => percentCell(info.getValue(), "favorable"),
  }),
  columnHelper.accessor("downside", {
    header: "Downside",
    cell: (info) => percentCell(info.getValue(), "adverse"),
  }),
  columnHelper.accessor("giveback", {
    header: "Giveback",
    cell: (info) => percentCell(info.getValue(), "adverse"),
  }),
  columnHelper.accessor("recovery", {
    header: "Recovery",
    cell: (info) => percentCell(info.getValue(), "favorable"),
  }),
  columnHelper.accessor("session_range", {
    header: "Range",
    cell: (info) => percentCell(info.getValue()),
  }),
  columnHelper.accessor("true_upside", {
    header: "True Upside",
    cell: (info) => percentCell(info.getValue(), "favorable"),
  }),
  columnHelper.accessor("true_downside", {
    header: "True Downside",
    cell: (info) => percentCell(info.getValue(), "adverse"),
  }),
  columnHelper.accessor("true_range", {
    header: "True Range",
    cell: (info) => percentCell(info.getValue()),
  }),
];

export function OpportunityTable({ data }: { data: OpportunityTargetRow[] }) {
  // TanStack Table intentionally returns function-heavy objects; this component is not memoized.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({ data, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <div className="table-wrap opportunity-table-wrap">
      <table>
        <thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id}>
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
