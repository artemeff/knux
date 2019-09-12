defmodule Knux.MixProject do
  use Mix.Project

  def project do
    [
      app: :knux,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:connection, "~> 1.0.4"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
    ]
  end

  defp description do
    "Library for dealing with data structures"
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/artemeff/knux"}
    ]
  end
end
