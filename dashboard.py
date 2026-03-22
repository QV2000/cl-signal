"""
CL Signal Dashboard - Price vs Signal Visualization

Run locally: python dashboard.py
Requires: pip install plotly pandas
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb

# For remote database access
DB_PATH = os.environ.get("CL_SIGNAL_DB_PATH", "cl_signal.ddb")


def get_price_data(conn, hours=24):
    """Get price data from orderbook snapshots (best bid)."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    df = conn.execute("""
        SELECT ts, price
        FROM orderbook_snapshots
        WHERE side = 'B' AND level = 0 AND ts >= ?
        ORDER BY ts
    """, [cutoff]).df()

    if df.empty:
        # Fallback to fills
        df = conn.execute("""
            SELECT ts, px as price
            FROM fills
            WHERE ts >= ?
            ORDER BY ts
        """, [cutoff]).df()

    # Resample to 1-minute bars for cleaner chart
    if not df.empty:
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index('ts').resample('1min').last().dropna().reset_index()

    return df


def get_signal_data(conn, hours=24):
    """Get signal history."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    df = conn.execute("""
        SELECT signal_ts as ts, composite_signal, direction, confidence,
               scored_wallets_count, delta_1h
        FROM signals
        WHERE signal_ts >= ?
        ORDER BY signal_ts
    """, [cutoff]).df()

    return df


def create_dashboard(conn, hours=24):
    """Create interactive price vs signal chart."""
    price_df = get_price_data(conn, hours)
    signal_df = get_signal_data(conn, hours)

    if price_df.empty:
        print("No price data available yet.")
        return None

    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.3, 0.2],
        subplot_titles=('CL Price', 'Composite Signal', 'Signal Delta 1h')
    )

    # Price chart
    fig.add_trace(
        go.Scatter(
            x=price_df['ts'],
            y=price_df['price'],
            mode='lines',
            name='CL Price',
            line=dict(color='#2196F3', width=1.5)
        ),
        row=1, col=1
    )

    # Signal chart with color based on direction
    if not signal_df.empty:
        colors = signal_df['composite_signal'].apply(
            lambda x: '#4CAF50' if x > 0.3 else ('#F44336' if x < -0.3 else '#9E9E9E')
        )

        fig.add_trace(
            go.Bar(
                x=signal_df['ts'],
                y=signal_df['composite_signal'],
                name='Signal',
                marker_color=colors,
                opacity=0.8
            ),
            row=2, col=1
        )

        # Add threshold lines
        fig.add_hline(y=0.3, line_dash="dash", line_color="green", row=2, col=1)
        fig.add_hline(y=-0.3, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=0, line_dash="solid", line_color="gray", row=2, col=1)

        # Delta chart
        if 'delta_1h' in signal_df.columns:
            delta_colors = signal_df['delta_1h'].apply(
                lambda x: '#4CAF50' if x and x > 0 else '#F44336' if x else '#9E9E9E'
            )
            fig.add_trace(
                go.Bar(
                    x=signal_df['ts'],
                    y=signal_df['delta_1h'].fillna(0),
                    name='Delta 1h',
                    marker_color=delta_colors,
                    opacity=0.7
                ),
                row=3, col=1
            )

    # Layout
    fig.update_layout(
        title=f'CL Signal Dashboard (Last {hours}h)',
        height=800,
        showlegend=True,
        template='plotly_dark',
        xaxis3_title='Time (UTC)',
    )

    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Signal", range=[-1.1, 1.1], row=2, col=1)
    fig.update_yaxes(title_text="Delta", row=3, col=1)

    return fig


def print_summary(conn):
    """Print current signal summary."""
    result = conn.execute("""
        SELECT signal_ts, composite_signal, direction, confidence,
               scored_wallets_count, delta_1h
        FROM signals
        ORDER BY signal_ts DESC
        LIMIT 1
    """).fetchone()

    if result:
        print(f"\n{'='*50}")
        print(f"LATEST SIGNAL: {result[2]} ({result[1]:+.3f})")
        print(f"{'='*50}")
        print(f"Time:       {result[0]} UTC")
        print(f"Confidence: {result[3]:.1%}")
        print(f"Wallets:    {result[4]}")
        print(f"Delta 1h:   {result[5]:+.3f}" if result[5] else "Delta 1h:   N/A")
        print(f"{'='*50}\n")
    else:
        print("No signals recorded yet.")


def main():
    # Check if running with remote DB
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = DB_PATH

    print(f"Connecting to: {db_path}")

    try:
        conn = duckdb.connect(db_path, read_only=True)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("\nTo use with remote server, first copy the database:")
        print("  scp root@89.167.113.150:/data/cl_signal.ddb ./cl_signal.ddb")
        print("  python dashboard.py cl_signal.ddb")
        return

    # Print summary
    print_summary(conn)

    # Create and show chart
    hours = 24 if len(sys.argv) < 3 else int(sys.argv[2])
    fig = create_dashboard(conn, hours)

    if fig:
        # Save to HTML
        output_file = "cl_signal_dashboard.html"
        fig.write_html(output_file)
        print(f"Dashboard saved to: {output_file}")

        # Try to open in browser
        try:
            import webbrowser
            webbrowser.open(f"file://{os.path.abspath(output_file)}")
        except:
            pass

    conn.close()


if __name__ == "__main__":
    main()
