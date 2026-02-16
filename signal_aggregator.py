"""
Signal Aggregation Engine
Converts wallet profiles into actionable trading signals
"""

from typing import Dict, List, Tuple
import numpy as np

class SignalAggregator:
    """
    Aggregates individual wallet positions into market-level signals
    
    Signal Philosophy:
    - Weight votes by wallet quality (composite score)
    - Require minimum threshold of qualified wallets
    - Measure consensus strength
    - Account for position size (conviction weighting)
    """
    
    def __init__(self, min_wallets: int = 3):
        self.min_wallets = min_wallets
        
    def aggregate_signal(self, wallet_profiles: List[Dict]) -> Dict:
        """
        Generate market-level trading signal from wallet profiles
        
        Returns:
            {
                'signal': 'BUY_YES' | 'BUY_NO' | 'NO_CLEAR_SIGNAL',
                'confidence': 0.0-10.0,
                'reasoning': str,
                'yes_weighted_score': float,
                'no_weighted_score': float,
                'yes_count': int,
                'no_count': int,
                'top_holders_consensus': str
            }
        """
        if len(wallet_profiles) < self.min_wallets:
            return {
                'signal': 'NO_CLEAR_SIGNAL',
                'confidence': 0.0,
                'reasoning': f'Insufficient qualified wallets (found {len(wallet_profiles)}, need {self.min_wallets}+)',
                'yes_weighted_score': 0,
                'no_weighted_score': 0,
                'yes_count': 0,
                'no_count': 0,
                'top_holders_consensus': 'N/A'
            }
        
        # Separate YES and NO positions
        yes_wallets = [w for w in wallet_profiles if w['position_outcome'].lower() == 'yes']
        no_wallets = [w for w in wallet_profiles if w['position_outcome'].lower() == 'no']
        
        # Calculate weighted scores (quality * conviction)
        yes_weighted_score = sum(
            w['composite_score'] * w['conviction_score'] * np.sqrt(w['position_size'])
            for w in yes_wallets
        )
        
        no_weighted_score = sum(
            w['composite_score'] * w['conviction_score'] * np.sqrt(w['position_size'])
            for w in no_wallets
        )
        
        # Normalize scores
        total_weighted = yes_weighted_score + no_weighted_score
        if total_weighted > 0:
            yes_pct = yes_weighted_score / total_weighted
            no_pct = no_weighted_score / total_weighted
        else:
            yes_pct = no_pct = 0.5
        
        # Check top holder consensus (top 5 wallets)
        top_5 = wallet_profiles[:min(5, len(wallet_profiles))]
        top_yes = sum(1 for w in top_5 if w['position_outcome'].lower() == 'yes')
        top_no = len(top_5) - top_yes
        
        if top_yes >= 4:
            top_consensus = f"Strong YES ({top_yes}/5)"
        elif top_no >= 4:
            top_consensus = f"Strong NO ({top_no}/5)"
        elif top_yes >= 3:
            top_consensus = f"Lean YES ({top_yes}/5)"
        elif top_no >= 3:
            top_consensus = f"Lean NO ({top_no}/5)"
        else:
            top_consensus = "Split"
        
        # Generate signal
        signal, confidence, reasoning = self._determine_signal(
            yes_pct, no_pct, yes_weighted_score, no_weighted_score,
            len(yes_wallets), len(no_wallets), top_consensus, wallet_profiles
        )
        
        return {
            'signal': signal,
            'confidence': confidence,
            'reasoning': reasoning,
            'yes_weighted_score': yes_weighted_score,
            'no_weighted_score': no_weighted_score,
            'yes_percentage': yes_pct * 100,
            'no_percentage': no_pct * 100,
            'yes_count': len(yes_wallets),
            'no_count': len(no_wallets),
            'top_holders_consensus': top_consensus,
            'total_qualified_wallets': len(wallet_profiles)
        }
    
    def _determine_signal(self, yes_pct: float, no_pct: float, 
                         yes_weighted: float, no_weighted: float,
                         yes_count: int, no_count: int,
                         top_consensus: str, all_profiles: List[Dict]) -> Tuple[str, float, str]:
        """
        Determine trading signal and confidence level
        
        Signal Thresholds:
        - Strong signal: 70%+ weighted, confidence 7-10
        - Moderate signal: 60-70% weighted, confidence 5-7
        - Weak signal: 55-60% weighted, confidence 3-5
        - No signal: <55% weighted
        """
        # Calculate base confidence from percentage spread
        spread = abs(yes_pct - no_pct)
        base_confidence = spread * 10  # 70% vs 30% = 4.0 confidence
        
        # Adjust confidence based on sample size
        total_wallets = len(all_profiles)
        sample_multiplier = min(total_wallets / 10, 1.5)  # More wallets = more confidence, cap at 1.5x
        
        # Adjust for top holder consensus
        if "Strong" in top_consensus:
            consensus_bonus = 2.0
        elif "Lean" in top_consensus:
            consensus_bonus = 1.0
        else:
            consensus_bonus = 0.0
        
        # Calculate final confidence (0-10 scale)
        confidence = min((base_confidence * sample_multiplier) + consensus_bonus, 10.0)
        
        # Determine signal
        if yes_pct >= 0.70:
            signal = "BUY_YES"
            strength = "STRONG"
        elif yes_pct >= 0.60:
            signal = "BUY_YES"
            strength = "MODERATE"
        elif yes_pct >= 0.55:
            signal = "BUY_YES"
            strength = "WEAK"
        elif no_pct >= 0.70:
            signal = "BUY_NO"
            strength = "STRONG"
        elif no_pct >= 0.60:
            signal = "BUY_NO"
            strength = "MODERATE"
        elif no_pct >= 0.55:
            signal = "BUY_NO"
            strength = "WEAK"
        else:
            signal = "NO_CLEAR_SIGNAL"
            strength = "N/A"
        
        # Generate reasoning
        reasoning = self._generate_reasoning(
            signal, strength, yes_count, no_count, yes_pct, no_pct,
            top_consensus, all_profiles
        )
        
        return signal, confidence, reasoning
    
    def _generate_reasoning(self, signal: str, strength: str, 
                          yes_count: int, no_count: int,
                          yes_pct: float, no_pct: float,
                          top_consensus: str, all_profiles: List[Dict]) -> str:
        """Generate human-readable reasoning for the signal"""
        
        if signal == "NO_CLEAR_SIGNAL":
            return (f"Smart money is split roughly evenly: {yes_count} YES vs {no_count} NO. "
                   f"Weighted scores: {yes_pct:.1%} YES vs {no_pct:.1%} NO. "
                   f"Insufficient consensus to generate a confident signal.")
        
        # Get average quality of top holders
        top_5 = all_profiles[:min(5, len(all_profiles))]
        avg_quality = np.mean([w['composite_score'] for w in top_5])
        
        # Get dominant side
        dominant_side = "YES" if "YES" in signal else "NO"
        dominant_count = yes_count if dominant_side == "YES" else no_count
        dominant_pct = yes_pct if dominant_side == "YES" else no_pct
        
        reasoning_parts = []
        
        # Main statement
        reasoning_parts.append(
            f"{strength} {signal} signal: {dominant_count} of {yes_count + no_count} qualified wallets "
            f"are positioned {dominant_side} (weighted score: {dominant_pct:.1%})."
        )
        
        # Top holder consensus
        reasoning_parts.append(f"Top holder consensus: {top_consensus}.")
        
        # Quality assessment
        if avg_quality >= 0.7:
            reasoning_parts.append("Top holders are high quality (avg score: {:.2f}).".format(avg_quality))
        elif avg_quality >= 0.5:
            reasoning_parts.append("Top holders are moderate quality (avg score: {:.2f}).".format(avg_quality))
        
        # Conviction assessment
        high_conviction = [w for w in all_profiles[:10] if w['conviction_score'] >= 0.75]
        if len(high_conviction) >= 3:
            reasoning_parts.append(
                f"{len(high_conviction)} of top 10 holders are showing high conviction "
                f"(oversized positions)."
            )
        
        return " ".join(reasoning_parts)
    
    def get_key_wallets(self, wallet_profiles: List[Dict], n: int = 5) -> List[Dict]:
        """Get top N most influential wallets"""
        return wallet_profiles[:n]
    
    def detect_whale_dominance(self, wallet_profiles: List[Dict]) -> Dict:
        """Check if a single whale dominates the signal"""
        if len(wallet_profiles) < 2:
            return {'is_dominated': False, 'whale': None}
        
        top_wallet = wallet_profiles[0]
        second_wallet = wallet_profiles[1]
        
        # Check if top wallet's position is 3x larger than second
        if top_wallet['position_size'] >= 3 * second_wallet['position_size']:
            return {
                'is_dominated': True,
                'whale': top_wallet,
                'dominance_factor': top_wallet['position_size'] / second_wallet['position_size']
            }
        
        return {'is_dominated': False, 'whale': None}