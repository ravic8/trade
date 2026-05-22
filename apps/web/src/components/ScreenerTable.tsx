import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router-dom";

import type { ScreenerResult } from "../api/types";
import { formatCurrencyCompact, formatDateTime } from "../utils/format";

const columnHelper = createColumnHelper<ScreenerResult>();

const columns = [
  columnHelper.accessor("ticker", {
    header: "Ticker",
    cell: (info) => (
      <Link className="ticker-link" to={`/symbols/${encodeURIComponent(info.getValue())}`}>
        {info.getValue()}
        <ArrowUpRight size={14} />
      </Link>
    ),
  }),
  columnHelper.accessor("exchange", { header: "Exchange" }),
  columnHelper.accessor("company", { header: "Company" }),
  columnHelper.accessor("liquidity", {
    header: "Median Value",
    cell: (info) => formatCurrencyCompact(info.getValue()),
  }),
  columnHelper.accessor("d5Up0100", { header: "5D Up 1%" }),
  columnHelper.accessor("d5Dn0100", { header: "5D Dn 1%" }),
  columnHelper.accessor("d5ClUp0200", { header: "Close Up 2%" }),
  columnHelper.accessor("d5VUp0200", { header: "Vol Up 2%" }),
  columnHelper.accessor("matchedAt", {
    header: "Matched",
    cell: (info) => formatDateTime(info.getValue()),
  }),
];

export function ScreenerTable({ data }: { data: ScreenerResult[] }) {
  // TanStack Table intentionally returns function-heavy objects; this component is not memoized.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <div className="table-wrap">
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
