from sqlalchemy import Column, Integer, String, Boolean, Date, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base


class State(Base):
    __tablename__ = "states"

    id = Column(Integer, primary_key=True)
    country_code = Column(String, nullable=True, default="US")
    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    source_url = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)


class SourceProvider(Base):
    __tablename__ = "source_providers"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    base_url = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)


class Game(Base):
    __tablename__ = "games"

    id = Column(Integer, primary_key=True)
    state_id = Column(Integer, nullable=True)
    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    game_type = Column(String, nullable=True)
    is_multi_state = Column(Boolean, default=False)
    has_bonus_ball = Column(Boolean, default=False)
    has_multiplier = Column(Boolean, default=False)
    has_secondary_draws = Column(Boolean, default=False)
    has_multiple_daily_draws = Column(Boolean, default=False)
    main_ball_count = Column(Integer, nullable=True)
    main_ball_min = Column(Integer, nullable=True)
    main_ball_max = Column(Integer, nullable=True)
    bonus_ball_min = Column(Integer, nullable=True)
    bonus_ball_max = Column(Integer, nullable=True)
    source_result_url = Column(Text, nullable=True)
    supports_history = Column(Boolean, default=True)
    supports_stats = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)


class GameSource(Base):
    __tablename__ = "game_sources"

    id = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey("games.id"), nullable=False)
    provider_id = Column(Integer, ForeignKey("source_providers.id"), nullable=False)
    source_url = Column(Text, nullable=False)
    source_role = Column(String, nullable=False, default="results")
    priority = Column(Integer, default=1)
    is_active = Column(Boolean, default=True)


class Draw(Base):
    __tablename__ = "draws"

    id = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey("games.id"), nullable=False)
    draw_date = Column(Date, nullable=False)
    draw_type = Column(String, nullable=False, default="main")
    draw_time = Column(String, nullable=True)

    main_numbers = Column(JSONB, nullable=True)
    bonus_number = Column(String, nullable=True)
    multiplier = Column(String, nullable=True)

    jackpot = Column(String, nullable=True)
    jackpot_change = Column(String, nullable=True)

    cash_payout = Column(String, nullable=True)
    secondary_draws = Column(JSONB, nullable=True)

    notes = Column(Text, nullable=True)
    source_url = Column(Text, nullable=True)

    next_draw_text = Column(Text, nullable=True)
    next_draw_timezone = Column(String, nullable=True)
    next_draw_relative = Column(String, nullable=True)