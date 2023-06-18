def test_clean_player_names(clean_player_names_data):
    assert len(clean_player_names_data) == 5
    assert clean_player_names_data["player"][0] == "Marcus Morris"
    assert clean_player_names_data["player"][1] == "Kelly Oubre"
    assert clean_player_names_data["player"][2] == "Gary Payton"
    assert clean_player_names_data["player"][3] == "Robert Williams"
    assert clean_player_names_data["player"][4] == "Lonnie Walker"
