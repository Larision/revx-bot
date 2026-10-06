from cli import InputCancelled, run_cli

if __name__ == "__main__":
    try:
        run_cli()
    except InputCancelled:
        print("\nEntrada cancelada con ESC.")
