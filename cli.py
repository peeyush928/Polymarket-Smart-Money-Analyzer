#!/usr/bin/env python3
"""
Command-Line Interface for Polymarket Smart Money Analyzer

Usage:
    python cli.py <market_url>
    python cli.py <market_url> --mock
    python cli.py <market_url> --min-profit 10000
"""

import sys
import argparse
from analyzer import PolymarketAnalyzer

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Polymarket smart money positions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python cli.py https://polymarket.com/event/bitcoin-100k
    python cli.py https://polymarket.com/event/trump-2024 --min-profit 10000
    python cli.py test --mock
        """
    )
    
    parser.add_argument(
        'market_url',
        help='Polymarket market URL to analyze'
    )
    
    parser.add_argument(
        '--min-profit',
        type=float,
        default=5000,
        help='Minimum total profit threshold for wallets (default: 5000)'
    )
    
    parser.add_argument(
        '--mock',
        action='store_true',
        help='Use mock data for testing (no API calls)'
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=20,
        help='Number of top wallets to display (default: 20)'
    )
    
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON instead of formatted text'
    )
    
    args = parser.parse_args()
    
    # Initialize analyzer
    print("🎯 Polymarket Smart Money Analyzer")
    print("=" * 70)
    print()
    
    analyzer = PolymarketAnalyzer(
        min_profit_threshold=args.min_profit,
        use_mock_data=args.mock
    )
    
    # Progress callback
    def show_progress(message):
        print(f"  {message}")
    
    # Run analysis
    print(f"📊 Analyzing: {args.market_url}")
    print()
    
    if args.mock:
        print("⚠️  Using mock data (test mode)")
        print()
    
    try:
        analysis = analyzer.analyze_market(args.market_url, progress_callback=show_progress)
        
        if not analysis.get('success'):
            print(f"\n❌ Analysis failed: {analysis.get('error', 'Unknown error')}")
            sys.exit(1)
        
        # Output results
        if args.json:
            import json
            print(json.dumps(analysis, indent=2, default=str))
        else:
            report = analyzer.format_analysis_report(analysis)
            print("\n" + report)
            
            # Additional CLI-specific summary
            print("\n" + "=" * 70)
            print("💡 QUICK SUMMARY")
            print("=" * 70)
            
            signal = analysis['signal']
            
            if signal['signal'] == 'BUY_YES':
                action = "BUY YES SHARES"
                emoji = "✅"
            elif signal['signal'] == 'BUY_NO':
                action = "BUY NO SHARES"
                emoji = "❌"
            else:
                action = "STAY OUT / WAIT FOR CLARITY"
                emoji = "⚠️"
            
            print(f"\n{emoji} Recommended Action: {action}")
            print(f"📊 Confidence Level: {signal['confidence']:.1f}/10")
            print(f"🎯 Based on {analysis['statistics']['qualified_wallets']} qualified wallets")
            
            whale = analysis['whale_dominance']
            if whale['is_dominated']:
                print(f"\n⚠️  WARNING: Whale dominates ({whale['dominance_factor']:.1f}x larger than #2)")
            
            print("\n" + "=" * 70)
            print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        if '--debug' in sys.argv:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()