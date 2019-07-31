defmodule Knux.MixProject do
  use Mix.Project

  def project do
    [
      app: :knux,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:connection, "~> 1.0.4"}
    ]
  end
end
