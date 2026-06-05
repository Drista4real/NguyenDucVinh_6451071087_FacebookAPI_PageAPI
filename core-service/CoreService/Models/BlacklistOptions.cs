namespace CoreService.Models;

public class BlacklistOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public int MaxViolationsBeforeBlacklist { get; set; } = 3;
}
