import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { createBrowserRouter, Navigate, RouterProvider } from "react-router-dom";

import { AppShell } from "./shell/AppShell";
import { DataPipelinePage } from "./pages/DataPipelinePage";
import { DashboardPage } from "./pages/DashboardPage";
import { FactorResearchPage } from "./pages/FactorResearchPage";
import { JobsPage } from "./pages/JobsPage";
import { MLResearchPage } from "./pages/MLResearchPage";
import { ResearchPage } from "./pages/ResearchPage";
import { ResearchProgressPage } from "./pages/ResearchProgressPage";
import { ScreenersPage } from "./pages/ScreenersPage";
import { SymbolPage } from "./pages/SymbolPage";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
    },
  },
});

const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { path: "/", element: <Navigate to="/dashboard" replace /> },
      { path: "/dashboard", element: <DashboardPage /> },
      { path: "/data", element: <DataPipelinePage /> },
      { path: "/screeners", element: <ScreenersPage /> },
      { path: "/symbols/:ticker", element: <SymbolPage /> },
      { path: "/research", element: <ResearchPage /> },
      { path: "/research/progress", element: <ResearchProgressPage /> },
      { path: "/research/factors", element: <FactorResearchPage /> },
      { path: "/research/models", element: <MLResearchPage /> },
      { path: "/jobs", element: <JobsPage /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>,
);
