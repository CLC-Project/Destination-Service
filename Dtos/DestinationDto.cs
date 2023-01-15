using DestinationService.Entities;

namespace DestinationService.Dtos
{
    public class DestinationDto
    {
        public DestinationDto(Destination destination)
        {
            Id = destination?.Id.ToString();
            Country = destination?.Country;
            Region = destination?.Region;
            City = destination?.City;
            User = destination?.User;
        }

        public string Id { get; set; }

        public string Country { get; set; }

        public string Region { get; set; }

        public string City { get; set; }

        public string User { get; set; }

    }
}
